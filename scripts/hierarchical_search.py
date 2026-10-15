#!/usr/bin/env python
"""
Helper for the hierarchical (two-stage) circular-orbit template-bank search in One Ring.

Stage 1 is the normal (coarse) peasoup template-bank search. This script then

  cluster : reads the stage-1 peasoup XML files of one filterbank/segment, maps every
            candidate to Legendre-coefficient (a_n) space, keeps only the local S/N peaks,
            builds a fine lattice of orbital templates around every peak and writes the
            lattice out as peasoup template-bank chunks (stage-2 input).

  merge   : reads the stage-2 peasoup XML files (one per template-bank chunk), concatenates
            the candidates, optionally clusters them again and writes a single peasoup-style
            XML file that the normal folding step can consume.

Only numpy and scipy are required (runs inside the PulsarX container).
"""
import argparse
import copy
import json
import logging
import os
import sys
import xml.etree.ElementTree as ET

import numpy as np
try:
    import scipy.linalg
    import scipy.spatial
    import scipy.special
    HAVE_SCIPY = True
except ImportError:          # the peasoup container has numpy only: enough for the fold search
    HAVE_SCIPY = False


def spherical_jn(n, x, derivative=False):
    """scipy.special.spherical_jn with a numpy fallback (Miller's downward recurrence) when scipy is missing."""
    if HAVE_SCIPY:
        return scipy.special.spherical_jn(n, x, derivative=derivative)
    x = np.asarray(x, dtype=np.float64)
    xs = np.where(x != 0, x, 1.0)
    nmax = max(n, 1)
    nstart = nmax + 30 + int(np.max(np.abs(x))) if x.size else nmax + 30
    jp1 = np.zeros_like(x)
    j = np.full_like(x, 1e-300)
    table = {}
    for k in range(nstart, -1, -1):          # j_{k-1} = (2k+1)/x * j_k - j_{k+1}
        if k <= nmax:
            table[k] = j
        jm1 = (2 * k + 1) / xs * j - jp1
        jp1, j = j, jm1
    j0 = np.where(x != 0, np.sin(x) / xs, 1.0)
    scale = j0 / np.where(table[0] != 0, table[0], 1.0)
    jn = table[n] * scale
    if not derivative:
        return np.where(x == 0, 1.0 if n == 0 else 0.0, jn)
    if n == 0:
        return -(table[1] * scale)
    d = table[n - 1] * scale - (n + 1) / xs * jn
    return np.where(x == 0, (1.0 / 3.0 if n == 1 else 0.0), d)


###############################################################################
# XML reading
###############################################################################

def read_keplerian_xml(xml_file):
    """
    Parse a peasoup Keplerian (template bank) search XML.

    Returns a dict with the observation metadata and one numpy array per candidate
    parameter. Orbital parameters are returned in the units used for the template bank:
    angular velocity (rad/s), projected semi-major axis (lt-s), orbital phase (rad).
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    header_params = root.find('header_parameters')
    search_params = root.find('search_parameters')
    segment_params = root.find('segment_parameters')
    candidates = root.find('candidates')

    tsamp = float(header_params.find('tsamp').text)
    segment_nsamples = int(segment_params.find('segment_nsamples').text)
    ntemplates_elem = search_params.find('ntemplates')

    info = dict(
        xml_file=xml_file,
        tsamp=tsamp,
        nsamples=int(header_params.find('nsamples').text),
        tstart=float(header_params.find('tstart').text),
        segment_start_sample=int(segment_params.find('segment_start_sample').text),
        segment_nsamples=segment_nsamples,
        segment_pepoch=float(segment_params.find('segment_pepoch').text),
        tobs=tsamp * segment_nsamples,
        ntemplates=int(ntemplates_elem.text) if ntemplates_elem is not None else 0,
        ncands=len(candidates),
    )

    cols = {k: [] for k in ('period', 'dm', 'pb', 'a1', 'phi', 't0', 'snr')}
    for cand in candidates:
        for k in cols:
            cols[k].append(float(cand.find(k).text))
    arrays = {k: np.asarray(v, dtype=np.float64) for k, v in cols.items()}

    # template coordinates: angular velocity (rad/s), asini/c (lt-s), orbital phase (rad)
    with np.errstate(divide='ignore'):
        omega = 2 * np.pi / (arrays['pb'] * 86400.)
    arrays['template'] = np.column_stack([omega, arrays['a1'], np.deg2rad(arrays['phi'])])
    return info, arrays, tree


def read_bank_header(bank_file):
    """
    Parse the '# KEY: value' header of a peasoup template bank (template_bank_generator format). Returns a dict
    with tobs_h, mismatch, spin_period_ms, porb_min_h, porb_max_h, min_pulsar_mass, max_companion_mass,
    inclination_fraction, phase_min, phase_max (missing keys are absent).
    """
    keys = {'TOBS (h)': 'tobs_h', 'MISMATCH': 'mismatch', 'SPIN PERIOD (ms)': 'spin_period_ms',
            'PORB MIN (h)': 'porb_min_h', 'PORB MAX (h)': 'porb_max_h', 'MIN PULSAR MASS (Msun)': 'min_pulsar_mass',
            'MAX COMPANION MASS (Msun)': 'max_companion_mass', 'INCLINATION ANGLE FRACTION': 'inclination_fraction',
            'ORBITAL PHASE MIN (rad)': 'phase_min', 'ORBITAL PHASE MAX (rad)': 'phase_max', 'NTEMPLATES': 'ntemplates'}
    hdr = {}
    with open(bank_file) as f:
        for line in f:
            if not line.startswith('#'):
                break
            body = line[1:].strip()
            if ':' not in body:
                continue
            key, val = body.rsplit(':', 1)
            key = key.strip()
            if key in keys:
                try:
                    hdr[keys[key]] = float(val)
                except ValueError:
                    pass
    return hdr


def bank_header_from_xml(xml_file):
    """Template bank path recorded in a peasoup XML and its parsed header ({} if unavailable)."""
    root = ET.parse(xml_file).getroot()
    elem = root.find('search_parameters/template_bank_file')
    path = elem.text.strip() if elem is not None and elem.text else None
    if path and os.path.exists(path):
        return path, read_bank_header(path)
    return path, {}


def resolve_bank_parameters(args, reference_xml, what='fine grid', need_period=True):
    """
    Fill nominal_mismatch, tobs and period_min from the template bank header of the stage-1 search when they are
    not given (None): MISMATCH, TOBS (h) and SPIN PERIOD (ms). Returns the header dict (for region clipping).
    """
    bank = getattr(args, 'template_bank', None)
    if bank:
        hdr = read_bank_header(bank)
    else:
        bank, hdr = bank_header_from_xml(reference_xml)
    if args.nominal_mismatch is None:
        if 'mismatch' not in hdr:
            logging.error("nominal_mismatch not given and no MISMATCH in the template bank header (%s)", bank); sys.exit(1)
        args.nominal_mismatch = hdr['mismatch']
    if args.tobs is None and 'tobs_h' in hdr:
        args.tobs = hdr['tobs_h'] * 3600.
    if need_period and args.period_min is None:
        if 'spin_period_ms' not in hdr:
            logging.error("period_min not given and no SPIN PERIOD in the template bank header (%s)", bank); sys.exit(1)
        args.period_min = hdr['spin_period_ms'] / 1000.
    logging.info("%s parameters: nominal_mismatch %g, tobs %s s%s (template bank: %s)", what, args.nominal_mismatch,
                 'segment length' if args.tobs is None else '%.1f' % args.tobs,
                 ', period_min %g s' % args.period_min if need_period else '', bank)
    return hdr


def a1_max_of_omega(omega, hdr):
    """Largest projected semi-major axis (lt-s) allowed by the bank's mass limits for an angular velocity omega."""
    G, MSUN, C = 6.67430e-11, 1.98847e30, 299792458.
    mp, mc = hdr['min_pulsar_mass'] * MSUN, hdr['max_companion_mass'] * MSUN
    sini = hdr.get('inclination_fraction', 1.0)
    return (G * mc ** 3 / (mp + mc) ** 2) ** (1. / 3.) * omega ** (-2. / 3.) / C * sini


def clip_to_bank_region(templates, owner, hdr, centers=None):
    """
    Keep only the fine-grid templates inside the physical region of the template bank: orbital period within
    [PORB MIN, PORB MAX], 0 < asini/c <= a1_max(omega) from the mass limits, orbital phase wrapped into [0, 2pi).
    A centre (stage-1 peak template) whose lattice is clipped away entirely is kept on its own, so that every
    peak is still searched at least at its stage-1 orbit.
    """
    t = templates.copy()
    keep = np.ones(len(t), dtype=bool)
    if 'porb_min_h' in hdr and 'porb_max_h' in hdr:
        keep &= (t[:, 0] <= 2 * np.pi / (hdr['porb_min_h'] * 3600.)) & (t[:, 0] >= 2 * np.pi / (hdr['porb_max_h'] * 3600.))
    if 'min_pulsar_mass' in hdr and 'max_companion_mass' in hdr:
        keep &= t[:, 1] <= a1_max_of_omega(np.maximum(t[:, 0], 1e-12), hdr)
    t[:, 2] = np.mod(t[:, 2], 2 * np.pi)
    t, owner = t[keep], owner[keep]
    if centers is not None:
        lost = np.setdiff1d(np.arange(len(centers)), owner)
        if len(lost):
            logging.warning("%d peak lattice(s) lie entirely outside the template bank region; keeping their centres only", len(lost))
            c = np.asarray(centers)[lost].copy()
            c[:, 2] = np.mod(c[:, 2], 2 * np.pi)
            t, owner = np.vstack([t, c]), np.concatenate([owner, lost])
    return t, owner


def read_many(xml_files):
    """Read several XML files and concatenate the candidate arrays."""
    infos, chunks, trees = [], [], []
    for f in xml_files:
        info, arrays, tree = read_keplerian_xml(f)
        logging.info("%s: %d candidates, tobs = %.1f s, ntemplates = %d",
                     f, info['ncands'], info['tobs'], info['ntemplates'])
        infos.append(info)
        chunks.append(arrays)
        trees.append(tree)
    keys = chunks[0].keys()
    merged = {k: np.concatenate([c[k] for c in chunks], axis=0) for k in keys}
    merged['file_index'] = np.concatenate([np.full(c['snr'].shape, i, dtype=int) for i, c in enumerate(chunks)])
    return infos, merged, trees


###############################################################################
# Legendre coefficient space
###############################################################################

def keplerian_to_an(template, tobs, spin_freq, order_highest, f0tol_fac):
    """
    Map circular-orbit templates to Legendre decomposition coefficients a_n.

    template  : (N, 3) array of [angular velocity (rad/s), asini/c (lt-s), orbital phase (rad)]
    tobs      : observation (segment) length in seconds
    spin_freq : spin frequency (Hz), scalar or (N,) array
    returns   : (N, order_highest) array; column 0 is the (down-weighted) spin-frequency term
    """
    spin_freq = np.broadcast_to(np.asarray(spin_freq, dtype=np.float64), (len(template),))
    vnu = spin_freq * tobs / 2.
    vs = template[:, 0] * tobs / 2.
    vr = 2 * np.pi * spin_freq * template[:, 1]
    vp = vs - template[:, 2]
    coordinate = np.empty(shape=(len(template), order_highest), dtype=np.float64)
    coordinate[:, 0] = f0tol_fac * (2 * np.pi * vnu / np.sqrt(3)
                                    - np.sqrt(3) * vr * spherical_jn(1, vs) * np.cos(vp))
    for n in range(2, order_highest + 1):
        coordinate[:, n - 1] = -np.sqrt(2 * n + 1) * vr * spherical_jn(n, vs) * np.sin(vp + n * np.pi / 2.)
    return coordinate


def circular_metric(template, freq_max, tobs, order_highest):
    """
    Induced metric of the circular-orbit parameter space (omega, tau, alpha) at each template.

    b_n = sqrt(2n+1) * r * j_n(s) * sin(p + n*pi/2) with r = 2*pi*f*tau, s = omega*T/2,
    p = omega*T/2 - alpha, and metric_ij = sum_n d_i b_n d_j b_n (n = 2 .. order_highest).
    Derivatives are analytic, so no symbolic algebra is needed. Returns an (N, 3, 3) array.
    """
    om, tau, al = template[:, 0], template[:, 1], template[:, 2]
    s = om * tobs / 2.
    r = 2 * np.pi * freq_max * tau
    p = om * tobs / 2. - al
    jac = np.zeros((len(template), order_highest - 1, 3), dtype=np.float64)
    for k, n in enumerate(range(2, order_highest + 1)):
        c = np.sqrt(2 * n + 1)
        jn = spherical_jn(n, s)
        djn = spherical_jn(n, s, derivative=True)
        sn = np.sin(p + n * np.pi / 2.)
        cs = np.cos(p + n * np.pi / 2.)
        jac[:, k, 0] = c * r * (djn * sn + jn * cs) * tobs / 2.   # d/d omega
        jac[:, k, 1] = c * 2 * np.pi * freq_max * jn * sn         # d/d tau
        jac[:, k, 2] = -c * r * jn * cs                            # d/d alpha
    return np.einsum('nki,nkj->nij', jac, jac)


###############################################################################
# Clustering
###############################################################################

def find_snr_peaks(an, snr, radius, min_prominence=0.0, workers=1):
    """
    Cluster candidates in a_n space and return the S/N peaks.

    Candidates are visited in order of decreasing S/N. A candidate with no brighter neighbour
    within 'radius' starts a new cluster and is its peak; otherwise it joins the cluster of its
    brightest neighbour. When a candidate touches several clusters its S/N is the saddle between
    them: every cluster whose peak rises less than 'min_prominence' above the saddle is absorbed
    into the cluster with the brightest peak. With min_prominence = 0 nothing is absorbed and the
    peaks are exactly the local S/N maxima (the original rule). Ties in S/N go to the lower index.

    Returns (idx_peak, prominence, nmembers): the candidate indices of the surviving peaks (in
    decreasing S/N), the prominence of each peak (inf if it never touches a brighter cluster) and
    the number of candidates in its cluster.
    """
    n = len(snr)
    tree = scipy.spatial.KDTree(an, leafsize=30)
    neighbours = tree.query_ball_point(an, r=radius, p=2, eps=0, workers=workers)

    order = np.argsort(-snr, kind='stable')
    rank = np.empty(n, dtype=np.int64)
    rank[order] = np.arange(n)
    parent = np.arange(n)                       # union-find; the root of a cluster is its peak
    is_peak = np.zeros(n, dtype=bool)
    prominence = np.full(n, np.inf)

    def find_roots(idx):
        roots = parent[idx]
        while True:
            up = parent[roots]
            if np.array_equal(up, roots):
                break
            roots = up
        parent[idx] = roots                     # path compression
        return roots

    for i in order:
        nbrs = np.asarray(neighbours[i])
        brighter = nbrs[rank[nbrs] < rank[i]]
        if brighter.size == 0:
            is_peak[i] = True
            continue
        roots = np.unique(find_roots(brighter))
        a = roots[np.argmin(rank[roots])]       # cluster with the brightest peak
        parent[i] = a
        for b in roots:
            if b == a:
                continue
            if np.isinf(prominence[b]):         # first contact with a brighter cluster = highest saddle
                prominence[b] = snr[b] - snr[i]
                if prominence[b] < min_prominence:
                    parent[b] = a
                    is_peak[b] = False

    idx_peak = order[is_peak[order]]
    roots = find_roots(np.arange(n))
    nmembers = np.bincount(roots, minlength=n)[idx_peak]
    return idx_peak, prominence[idx_peak], nmembers


def cluster_candidates(arrays, tobs, order_highest, f0tol_fac, nominal_mismatch, distance_tol,
                       min_prominence=0.0, workers=1):
    spin_freq = 1. / arrays['period']
    an = keplerian_to_an(arrays['template'], tobs, spin_freq, order_highest, f0tol_fac)
    radius = distance_tol * np.sqrt(nominal_mismatch)
    return find_snr_peaks(an, arrays['snr'], radius, min_prominence=min_prominence, workers=workers)


###############################################################################
# Fine lattice around every peak
###############################################################################

def eigen_lattice(centers, metrics, halfrange, stepsize):
    """
    Build a lattice in the eigen-basis of the metric around every centre.

    centers  : (N, 3) template coordinates
    metrics  : (N, 3, 3) metric at each centre
    halfrange, stepsize : in units of sqrt(mismatch)
    returns  : (M, 3) array of templates (omega > 0 and tau > 0 only) and the (M,) index of the
               centre each template belongs to
    """
    ndim = 3
    nstep = 2 * int(np.round(halfrange / stepsize)) + 1
    grid = np.linspace(-(nstep - 1) / 2 * stepsize, (nstep - 1) / 2 * stepsize, nstep, endpoint=True)
    samples_eigen = np.array(np.meshgrid(grid, grid, grid, indexing='ij')).reshape(ndim, -1)
    samples_eigen = samples_eigen[:, np.sum(samples_eigen ** 2, axis=0) < halfrange ** 2]   # (3, M)

    eigenvalue, eigenvector = np.linalg.eigh(metrics)                      # (N, 3), (N, 3, 3)
    if np.any(eigenvalue <= 0):
        bad = np.sum(np.any(eigenvalue <= 0, axis=1))
        logging.warning("%d metrics are not positive definite; clipping eigenvalues", bad)
        eigenvalue = np.clip(eigenvalue, np.finfo(np.float64).tiny, None)
    transform = eigenvector / np.sqrt(eigenvalue)[:, None, :]              # (N, 3, 3)
    offsets = np.einsum('nij,jm->nmi', transform, samples_eigen)           # (N, M, 3)
    samples = centers[:, None, :] + offsets
    nper = samples.shape[1]
    samples = samples.reshape(-1, ndim)
    owner = np.repeat(np.arange(len(centers)), nper)
    keep = (samples[:, 0] > 0) & (samples[:, 1] > 0)
    return samples[keep], owner[keep]


def orbital_an(template, freq_max, tobs, order_highest):
    """Legendre coefficients of the orbital phase only (orders 2 .. order_highest), the space the metric lives in."""
    return keplerian_to_an(template, tobs, freq_max, order_highest, 0.0)[:, 1:]


def _greedy_thin(points, radius):
    """
    Keep a subset of 'points' such that no two kept points are closer than 'radius'. Points are visited in
    order (earlier wins), so sort them by priority first. Vectorised rounds over the collision graph.
    """
    n = len(points)
    keep = np.zeros(n, dtype=bool)
    alive = np.ones(n, dtype=bool)
    tree = scipy.spatial.cKDTree(points)
    coo = tree.sparse_distance_matrix(tree, radius, output_type='coo_matrix')
    row, col = coo.row, coo.col
    while alive.any():
        # a point is accepted when no alive point with a smaller index collides with it
        best = np.full(n, n, dtype=np.int64)
        mask = alive[row] & alive[col]
        np.minimum.at(best, row[mask], col[mask])
        idx = np.flatnonzero(alive)
        accept = idx[best[idx] >= idx]
        keep[accept] = True
        # the accepted points and everything colliding with them leave the pool
        dead = np.zeros(n, dtype=bool)
        dead[accept] = True
        hit = np.isin(row, accept) & mask
        dead[col[hit]] = True
        alive &= ~dead
    return keep


def fill_gaps(centers, templates, owner, metric_fn, an_fn, halfrange, cover_radius, rounds=4, samples_per_template=8,
              batch_size=2000, rng=None):
    """
    Poisson-disk fill-in: sample random points of every centre's region (ball of radius 'halfrange' in a_n space)
    and add a template wherever the nearest existing template is farther than 'cover_radius'. Added points are
    thinned so that they are at least 'cover_radius' apart. Repeated for 'rounds' rounds or until nothing is added,
    which leaves every sampled point within 'cover_radius' of a template, i.e. a mismatch <= cover_radius**2.
    Returns the extended (templates, owner).
    """
    rng = np.random.default_rng(0) if rng is None else rng
    out_par, out_owner = [templates], [owner]
    for b0 in range(0, len(centers), batch_size):
        cen = centers[b0:b0 + batch_size]
        nb = len(cen)
        sel = (owner >= b0) & (owner < b0 + nb)
        cur_par, cur_owner = templates[sel], owner[sel] - b0
        cur_an = an_fn(cur_par)
        an0 = an_fn(cen)
        eigenvalue, eigenvector = np.linalg.eigh(metric_fn(cen))
        eigenvalue = np.clip(eigenvalue, np.finfo(np.float64).tiny, None)
        transform = eigenvector / np.sqrt(eigenvalue)[:, None, :]              # (nb, 3, 3)
        sep = 4.0 * halfrange
        for _ in range(rounds):
            # candidates: uniform in the metric ball at each centre (slightly enlarged), kept inside the a_n ball
            nper = np.maximum(50, samples_per_template * np.bincount(cur_owner, minlength=nb))
            cand_owner = np.repeat(np.arange(nb), nper)
            u = rng.normal(size=(len(cand_owner), 3))
            u /= np.linalg.norm(u, axis=1)[:, None]
            u *= 1.2 * halfrange * rng.random(len(cand_owner))[:, None] ** (1. / 3.)
            cand_par = cen[cand_owner] + np.einsum('nij,nj->ni', transform[cand_owner], u)
            ok = (cand_par[:, 0] > 0) & (cand_par[:, 1] > 0)
            cand_par, cand_owner = cand_par[ok], cand_owner[ok]
            cand_an = an_fn(cand_par)
            inside = np.linalg.norm(cand_an - an0[cand_owner], axis=1) <= halfrange
            cand_par, cand_owner, cand_an = cand_par[inside], cand_owner[inside], cand_an[inside]
            if len(cand_par) == 0:
                break
            tree = scipy.spatial.cKDTree(np.hstack([cur_an, cur_owner[:, None] * sep]))
            key_cand = np.hstack([cand_an, cand_owner[:, None] * sep])
            d, _ = tree.query(key_cand)
            uncovered = d > cover_radius
            if not uncovered.any():
                break
            cand_par, cand_owner, cand_an, key_cand = cand_par[uncovered], cand_owner[uncovered], cand_an[uncovered], key_cand[uncovered]
            keep = _greedy_thin(key_cand, cover_radius)
            cur_par = np.concatenate([cur_par, cand_par[keep]])
            cur_an = np.concatenate([cur_an, cand_an[keep]])
            cur_owner = np.concatenate([cur_owner, cand_owner[keep]])
            out_par.append(cand_par[keep])
            out_owner.append(cand_owner[keep] + b0)
    return np.concatenate(out_par), np.concatenate(out_owner)


def advancing_front_lattice(centers, metric_fn, an_fn, halfrange, stepsize, batch_size=2000, max_layers=None,
                            step_correction=True, dup_fraction=0.7, fill=True):
    """
    Grow a lattice around every centre layer by layer (advancing front), re-evaluating the metric at each
    front point. Every distance test is done in Legendre-coefficient (a_n) space, where the mismatch is the
    squared Euclidean distance, so the result follows the curvature of the parameter manifold:

      - a front point proposes its 6 neighbours at distance 'stepsize' along the eigen-directions of the
        local metric; the step is rescaled once so that the true a_n distance equals 'stepsize',
      - a proposal is dropped if it lies farther than 'halfrange' (in a_n) from the centre, or within
        dup_fraction * stepsize of a template already placed around that centre (duplicates where the metric shrinks),
      - the surviving proposals form the next front; the region is filled when the front is empty,
      - finally (fill=True) a Poisson-disk pass adds templates wherever random points of the region are farther
        than the covering radius of the cubic lattice (stepsize * sqrt(3)/2) from every template, which closes the
        seams where fronts coming from different directions meet.

    metric_fn(templates) -> (N, 3, 3); an_fn(templates) -> (N, D). Returns (templates, owner) like eigen_lattice.
    Centres are processed in batches to bound memory; peaks are independent so batching changes nothing.
    """
    ndim = 3
    dup_radius = dup_fraction * stepsize
    out_par, out_owner = [], []
    signs = np.array([1., -1.])
    for b0 in range(0, len(centers), batch_size):
        cen = centers[b0:b0 + batch_size]
        nb = len(cen)
        an0 = an_fn(cen)
        acc_par = [cen]
        acc_an = [an0]
        acc_owner = [np.arange(nb)]
        front_par, front_an, front_owner = cen, an0, np.arange(nb)
        layer = 0
        while len(front_par) > 0 and (max_layers is None or layer < max_layers):
            layer += 1
            eigenvalue, eigenvector = np.linalg.eigh(metric_fn(front_par))
            eigenvalue = np.clip(eigenvalue, np.finfo(np.float64).tiny, None)
            dirs = eigenvector / np.sqrt(eigenvalue)[:, None, :]                  # (F, 3, 3) unit metric steps
            # 6 proposals per front point: +/- each eigen-direction
            offs = np.concatenate([sg * dirs[:, :, i] for i in range(ndim) for sg in signs], axis=0)  # (6F, 3)
            parent = np.tile(np.arange(len(front_par)), 2 * ndim)
            prop_par = front_par[parent] + stepsize * offs
            prop_owner = front_owner[parent]
            parent_an = front_an[parent]
            prop_an = an_fn(prop_par)
            if step_correction:
                d = np.linalg.norm(prop_an - parent_an, axis=1)
                scale = np.where(d > 0, stepsize / np.maximum(d, 1e-300), 1.0)
                prop_par = front_par[parent] + (stepsize * offs) * scale[:, None]
                prop_an = an_fn(prop_par)
            keep = (prop_par[:, 0] > 0) & (prop_par[:, 1] > 0)
            dist_c = np.linalg.norm(prop_an - an0[prop_owner], axis=1)
            keep &= dist_c <= halfrange
            if not keep.any():
                break
            prop_par, prop_an, prop_owner, dist_c = prop_par[keep], prop_an[keep], prop_owner[keep], dist_c[keep]
            # separate the owners in the search space so that one tree serves every centre of the batch
            sep = 4.0 * halfrange
            key_acc = np.hstack([np.concatenate(acc_an), np.concatenate(acc_owner)[:, None] * sep])
            key_prop = np.hstack([prop_an, prop_owner[:, None] * sep])
            tree = scipy.spatial.cKDTree(key_acc)
            near = tree.query_ball_point(key_prop, r=dup_radius)
            free = np.array([len(x) == 0 for x in near], dtype=bool)
            if not free.any():
                break
            prop_par, prop_an, prop_owner, dist_c, key_prop = prop_par[free], prop_an[free], prop_owner[free], dist_c[free], key_prop[free]
            # thin duplicates within the layer, inner points first
            order = np.argsort(dist_c, kind='stable')
            keep_layer = _greedy_thin(key_prop[order], dup_radius)
            sel = order[keep_layer]
            front_par, front_an, front_owner = prop_par[sel], prop_an[sel], prop_owner[sel]
            acc_par.append(front_par)
            acc_an.append(front_an)
            acc_owner.append(front_owner)
        out_par.append(np.concatenate(acc_par))
        out_owner.append(np.concatenate(acc_owner) + b0)
    templates, owner = np.concatenate(out_par), np.concatenate(out_owner)
    if fill:
        templates, owner = fill_gaps(centers, templates, owner, metric_fn, an_fn, halfrange,
                                     cover_radius=stepsize * np.sqrt(3.) / 2., batch_size=batch_size)
    return templates, owner


def write_template_bank(templates, path, tobs):
    with open(path, 'w') as f:
        f.write('# TEMPLATE BANK TECHNIQUE: GRID\n')
        f.write('# TOBS (h): %f\n' % (tobs / 3600.))
        f.write('# NTEMPLATES: %d\n' % len(templates))
        f.write('# ANGULAR_VELOCITY (rad/s) ASINI/C (lt-s) ORB_PHASE (rad) PERIASTRON (rad) ECCENTRICITY\n')
        np.savetxt(f, templates, fmt='%.17g %.17g %.17g')


###############################################################################
# Sub-commands
###############################################################################

def cmd_cluster(args):
    infos, arrays, _ = read_many(args.input)
    ncands_total = len(arrays['snr'])
    if ncands_total == 0:
        logging.error("No candidates found in the input XML files.")
        sys.exit(1)
    if np.all(arrays['pb'] <= 0):
        logging.error("No Keplerian candidates found (pb = 0 everywhere). Stage 1 must be a template bank search.")
        sys.exit(1)

    bank_hdr = resolve_bank_parameters(args, args.input[0], 'clustering / fine grid')
    tobs = args.tobs if args.tobs is not None else infos[0]['tobs']
    logging.info("Using tobs = %.3f s for the Legendre coefficients and the metric", tobs)

    keep = arrays['pb'] > 0
    if args.min_snr is not None:
        keep &= arrays['snr'] >= args.min_snr
    arrays = {k: v[keep] for k, v in arrays.items()}
    logging.info("%d of %d candidates enter the clustering", len(arrays['snr']), ncands_total)

    idx_peak, prominence, nmembers = cluster_candidates(
        arrays, tobs, args.order_highest, args.f0tol_fac, args.nominal_mismatch, args.distance_tol,
        min_prominence=args.min_prominence, workers=args.workers)
    logging.info("%d S/N peaks after clustering (min_prominence = %g)", len(idx_peak), args.min_prominence)

    npeaks_clustered = len(idx_peak)
    # idx_peak is sorted by decreasing S/N, so the caps keep the brightest peaks
    if args.max_peaks is not None and len(idx_peak) > args.max_peaks:
        logging.info("keeping the %d brightest of %d peaks (max_peaks)", args.max_peaks, len(idx_peak))
        idx_peak, prominence, nmembers = idx_peak[:args.max_peaks], prominence[:args.max_peaks], nmembers[:args.max_peaks]

    peaks = {k: v[idx_peak] for k, v in arrays.items()}

    # Unique peak templates, kept in order of decreasing peak S/N (first occurrence)
    _, first, inverse = np.unique(peaks['template'], axis=0, return_index=True, return_inverse=True)
    inverse = np.asarray(inverse).ravel()
    rank_of_center = np.argsort(np.argsort(first))          # centre id -> position in S/N order
    centers = peaks['template'][np.sort(first)]
    peak_center = rank_of_center[inverse]                   # peak -> row of 'centers'
    logging.info("%d unique peak templates", len(centers))

    halfrange = args.search_halfrange_fac * np.sqrt(args.nominal_mismatch)
    stepsize = args.search_stepsize_fac * np.sqrt(args.nominal_mismatch)
    freq_max = 1. / args.period_min
    if args.lattice == 'adaptive':
        templates, owner = advancing_front_lattice(
            centers,
            lambda t: circular_metric(t, freq_max, tobs, args.order_highest),
            lambda t: orbital_an(t, freq_max, tobs, args.order_highest),
            halfrange, stepsize)
    else:
        metrics = circular_metric(centers, freq_max, tobs, args.order_highest)
        templates, owner = eigen_lattice(centers, metrics, halfrange, stepsize)
    if not args.no_bank_clip and bank_hdr:
        n0 = len(templates)
        templates, owner = clip_to_bank_region(templates, owner, bank_hdr, centers)
        logging.info("%d of %d templates kept inside the template bank's parameter region", len(templates), n0)
    logging.info("%d fine-grid templates generated with the %s lattice (%.1f per peak)",
                 len(templates), args.lattice, len(templates) / max(1, len(centers)))

    center_used = np.ones(len(centers), dtype=bool)
    if args.max_templates is not None and len(templates) > args.max_templates:
        per_center = np.bincount(owner, minlength=len(centers))
        center_used = np.cumsum(per_center) <= args.max_templates
        templates = templates[center_used[owner]]
        logging.info("keeping the %d brightest peaks / %d templates (max_templates = %d)",
                     center_used.sum(), len(templates), args.max_templates)
    peak_used = center_used[peak_center]

    with open(args.peaks_file, 'w') as f:
        f.write("period,dm,snr,prominence,nmembers,used,pb,a1,phi,t0,angular_velocity,asini_c,orb_phase_rad,file_index\n")
        for i in range(len(idx_peak)):
            f.write("%.15g,%.6f,%.4f,%.4f,%d,%d,%.15g,%.15g,%.15g,%.10f,%.15g,%.15g,%.15g,%d\n" % (
                peaks['period'][i], peaks['dm'][i], peaks['snr'][i], prominence[i], nmembers[i], peak_used[i],
                peaks['pb'][i], peaks['a1'][i], peaks['phi'][i], peaks['t0'][i],
                peaks['template'][i, 0], peaks['template'][i, 1], peaks['template'][i, 2],
                peaks['file_index'][i]))

    nchunks = 0 if args.no_bank_files else max(1, int(np.ceil(len(templates) / float(args.templates_per_chunk))))
    chunk_files = []
    for i in range(nchunks):
        chunk = templates[i * args.templates_per_chunk:(i + 1) * args.templates_per_chunk]
        path = "%s_ck%03d.txt" % (args.prefix, i)
        write_template_bank(chunk, path, tobs)
        chunk_files.append(path)
        logging.info("wrote %s with %d templates", path, len(chunk))

    summary = dict(
        input_files=[info['xml_file'] for info in infos],
        ncands_input=int(ncands_total),
        ncands_clustered=int(len(arrays['snr'])),
        npeaks_clustered=int(npeaks_clustered),
        npeaks=int(len(idx_peak)),
        nunique_peak_templates=int(len(centers)),
        npeak_templates_used=int(center_used.sum()),
        ntemplates=int(len(templates)),
        nchunks=nchunks,
        chunk_files=chunk_files,
        tobs=float(tobs),
        parameters=dict(order_highest=args.order_highest, f0tol_fac=args.f0tol_fac,
                        nominal_mismatch=args.nominal_mismatch, distance_tol=args.distance_tol,
                        search_halfrange_fac=args.search_halfrange_fac,
                        search_stepsize_fac=args.search_stepsize_fac, period_min=args.period_min,
                        lattice=args.lattice, min_snr=args.min_snr, min_prominence=args.min_prominence,
                        max_peaks=args.max_peaks, max_templates=args.max_templates,
                        templates_per_chunk=args.templates_per_chunk),
    )
    with open(args.summary_file, 'w') as f:
        json.dump(summary, f, indent=2)


def cmd_merge(args):
    infos, arrays, trees = read_many(args.input)
    ncands_total = len(arrays['snr'])
    resolve_bank_parameters(args, args.input[0], 'stage-2 clustering', need_period=False)
    tobs = args.tobs if args.tobs is not None else infos[0]['tobs']

    if ncands_total == 0:
        logging.warning("No candidates in any input file; writing an empty candidate list.")
        selected = np.array([], dtype=int)
    elif args.cluster:
        keep = arrays['pb'] > 0
        sub = {k: v[keep] for k, v in arrays.items()}
        idx, _, _ = cluster_candidates(sub, tobs, args.order_highest, args.f0tol_fac,
                                       args.nominal_mismatch, args.distance_tol,
                                       min_prominence=args.min_prominence, workers=args.workers)
        selected = np.flatnonzero(keep)[idx]
        logging.info("%d of %d candidates kept after clustering", len(selected), ncands_total)
    else:
        selected = np.arange(ncands_total)

    # sort by S/N, highest first
    selected = selected[np.argsort(-arrays['snr'][selected], kind='stable')]

    # Gather the candidate elements of every input file, in file order
    cand_elems = []
    for tree in trees:
        cand_elems.extend(list(tree.getroot().find('candidates')))

    out_tree = copy.deepcopy(trees[0])
    root = out_tree.getroot()
    candidates = root.find('candidates')
    for child in list(candidates):
        candidates.remove(child)
    for new_id, i in enumerate(selected):
        elem = copy.deepcopy(cand_elems[i])
        elem.set('id', str(new_id))
        candidates.append(elem)

    search_params = root.find('search_parameters')
    ntemplates_elem = search_params.find('ntemplates')
    if ntemplates_elem is not None:
        ntemplates_elem.text = str(sum(info['ntemplates'] for info in infos))

    # Bookkeeping goes at the end so that positional access to the standard sections keeps working
    merge_info = ET.SubElement(root, 'hierarchical_merge')
    ET.SubElement(merge_info, 'ninput_files').text = str(len(infos))
    ET.SubElement(merge_info, 'ncands_input').text = str(ncands_total)
    ET.SubElement(merge_info, 'ncands_output').text = str(len(selected))
    ET.SubElement(merge_info, 'clustered').text = str(int(bool(args.cluster)))
    for info in infos:
        ET.SubElement(merge_info, 'input_file').text = os.path.abspath(info['xml_file'])

    out_tree.write(args.output, encoding='utf-8', xml_declaration=True)
    logging.info("wrote %s with %d candidates", args.output, len(selected))


###############################################################################
# Stage 2 by folding: sub-integration profiles aligned per template (LOKI / pyloki scheme)
###############################################################################

def read_presto_inf(inf_file):
    """Parse a PRESTO-style .inf file (as written by peasoup -d) into a dict."""
    info = {}
    for line in open(inf_file):
        if '=' not in line:
            continue
        key, val = line.split('=', 1)
        key = key.strip().lower(); val = val.strip()
        if key.startswith('width of each time series bin'):
            info['tsamp'] = float(val)
        elif key.startswith('epoch of observation'):
            info['epoch'] = float(val)
        elif key.startswith('number of bins in the time series'):
            info['nsamples'] = int(val)
        elif key.startswith('dispersion measure'):
            info['dm'] = float(val)
    return info


def read_presto_timeseries(dat_file):
    """Read a peasoup/PRESTO .dat (raw float32) with its .inf; returns (data, info)."""
    inf_file = os.path.splitext(dat_file)[0] + '.inf'
    info = read_presto_inf(inf_file)
    data = np.fromfile(dat_file, dtype=np.float32)
    if 'nsamples' in info:
        data = data[:info['nsamples']]
    return data, info


def circular_delay(t, template):
    """Roemer delay (s) of a circular orbit, peasoup convention: tau * sin(omega*t - alpha), t from segment start."""
    return template[1] * np.sin(template[0] * t - template[2])


def emission_time(t_arr, template, niter=2):
    """Emission time of samples received at t_arr for the orbit 'template' (arrival = emission + R(emission))."""
    te = t_arr - circular_delay(t_arr, template)
    for _ in range(niter - 1):
        te = t_arr - circular_delay(te, template)
    return te


def normalise_timeseries(ts):
    """Zero-mean, unit-variance copy using robust estimates (median, MAD)."""
    ts = np.asarray(ts, dtype=np.float64)
    med = np.median(ts)
    mad = np.median(np.abs(ts - med)) * 1.4826
    if mad <= 0:
        mad = ts.std() or 1.0
    return ((ts - med) / mad).astype(np.float32)


def fold_subints(ts, tsamp, f0, template, nsub, nbins):
    """
    Fold a normalised time series with the orbital model 'template' at frequency f0 into nsub sub-integrations.
    Returns (E, N, t_sub): summed intensity and sample counts, shape (nsub, nbins), and the sub-integration
    centre times (s from the segment start).
    """
    n = len(ts)
    t_arr = (np.arange(n, dtype=np.float64) + 0.5) * tsamp
    te = emission_time(t_arr, template)
    phase = f0 * te
    ibin = (np.floor((phase - np.floor(phase)) * nbins)).astype(np.int64) % nbins
    isub = (np.arange(n, dtype=np.int64) * nsub) // n
    idx = isub * nbins + ibin
    E = np.bincount(idx, weights=ts, minlength=nsub * nbins).reshape(nsub, nbins)
    N = np.bincount(idx, minlength=nsub * nbins).reshape(nsub, nbins).astype(np.float64)
    t_sub = (np.arange(nsub) + 0.5) * (n * tsamp / nsub)
    return E, N, t_sub


def fold_subints_resampled(ts, tsamp, f0, nsub, nbins):
    """
    Fold a series already resampled into the emission frame of the peak orbit (peasoup convention: sample idx
    is emission time idx * tsamp) at constant frequency f0. Same outputs as fold_subints.
    """
    n = len(ts)
    phase = f0 * np.arange(n, dtype=np.float64) * tsamp
    ibin = (np.floor((phase - np.floor(phase)) * nbins)).astype(np.int64) % nbins
    isub = (np.arange(n, dtype=np.int64) * nsub) // n
    idx = isub * nbins + ibin
    E = np.bincount(idx, weights=ts, minlength=nsub * nbins).reshape(nsub, nbins)
    N = np.bincount(idx, minlength=nsub * nbins).reshape(nsub, nbins).astype(np.float64)
    t_sub = (np.arange(nsub) + 0.5) * (n * tsamp / nsub)
    return E, N, t_sub


def resample_batch_with_tool(resampler, dat_file, templates, mode='linear', workdir='resampled_tmp'):
    """
    Run the resample_timeseries tool (peasoup kernels) once for a batch of templates (the input series stays on
    the GPU for the whole batch). Returns the list of resampled .dat paths, in template order; the caller
    removes 'workdir' when done.
    """
    import subprocess
    os.makedirs(workdir, exist_ok=True)
    list_file = os.path.join(workdir, 'templates.txt')
    with open(list_file, 'w') as f:
        for t in templates:
            f.write('%.17g %.17g %.17g\n' % (t[0], t[1], t[2]))
    cmd = [resampler, '-i', dat_file, '--templates', list_file, '--outdir', workdir, '--mode', mode]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        logging.error("resampler failed: %s\n%s", ' '.join(cmd), res.stderr)
        sys.exit(1)
    files = [line.strip() for line in res.stdout.splitlines() if line.strip().endswith('.dat')]
    if len(files) != len(templates):
        logging.error("resampler returned %d files for %d templates", len(files), len(templates))
        sys.exit(1)
    return files


def box_widths(nbins, ducy_max=0.3):
    """Boxcar widths (bins) up to ducy_max * nbins, geometrically spaced like pyloki's trials."""
    w = [1]
    while True:
        nxt = max(w[-1] + 1, int(round(w[-1] * 1.5)))
        if nxt > max(1, int(ducy_max * nbins)):
            break
        w.append(nxt)
    return np.array(w, dtype=np.int64)


def boxcar_snr(profiles, widths, stdnoise=1.0):
    """
    pyloki's normalised boxcar matched filter for a batch of profiles (B, nbins) normalised to ~unit noise.
    stdnoise: noise standard deviation per bin; None estimates it per profile from the median absolute
    deviation of the bins (robust to the pulse), which also absorbs the variance change of interpolating
    resamplers. Returns (snr, width) of the best boxcar per profile.
    """
    B, size = profiles.shape
    if stdnoise is None:
        med = np.median(profiles, axis=1, keepdims=True)
        stdnoise = 1.4826 * np.median(np.abs(profiles - med), axis=1)
        stdnoise = np.where(stdnoise > 0, stdnoise, 1.0)
    profiles = profiles / np.asarray(stdnoise).reshape(-1, 1)
    total = profiles.sum(axis=1)
    doubled = np.concatenate([profiles, profiles], axis=1)
    cs = np.concatenate([np.zeros((B, 1), dtype=profiles.dtype), np.cumsum(doubled, axis=1)], axis=1)
    best = np.full(B, -np.inf); best_w = np.zeros(B, dtype=np.int64)
    for w in widths:
        if w >= size:
            break
        sums = cs[:, w:w + size] - cs[:, :size]           # circular boxcar sums for every start phase
        dmax = sums.max(axis=1)
        height = np.sqrt((size - w) / (size * w))
        b = w * height / (size - w)
        snr = (height + b) * dmax - b * total
        better = snr > best
        best[better] = snr[better]; best_w[better] = w
    return best, best_w


def template_shifts(templates, f0, peak_template, t_sub, dfreq=0.0, detrend=True):
    """
    Residual phase (turns) of every template relative to the fold model at the sub-integration centres:
    delta(t) = f0 * (R_peak(t) - R_template(t)) + dfreq * t.  Shape (ntemplates, nsub).

    With detrend=True the linear part c0 + c1*t of f0 * (R_peak - R_template) is removed and c1 is returned.
    A stage-1 FFT candidate found with the peak template reports f_obs = f_true + c1(true template), so after
    detrending the grid offset needed for the true template is only the FFT quantisation, and the frequency
    implied by a template at grid offset dfreq is f0 + dfreq - c1. Returns (shifts, slope).
    """
    r_peak = circular_delay(t_sub, peak_template)                                   # (nsub,)
    r_tmpl = templates[:, 1:2] * np.sin(templates[:, 0:1] * t_sub[None, :] - templates[:, 2:3])   # (T, nsub)
    resid = f0 * (r_peak[None, :] - r_tmpl)
    slope = np.zeros(len(templates))
    if detrend:
        tc = t_sub - t_sub.mean()
        slope = (resid - resid.mean(axis=1, keepdims=True)) @ tc / (tc @ tc)
        resid = resid - resid.mean(axis=1, keepdims=True) - slope[:, None] * tc[None, :]
    return resid + np.asarray(dfreq)[..., None] * t_sub[None, :], slope


def shift_add_score(E, N, shifts_turns, widths, batch=4096):
    """
    Rotate every sub-integration profile by -shift (Fourier phase rotation, exact for fractional bins), sum
    them, normalise to unit noise and score with the boxcar filter. shifts_turns: (T, nsub) in turns.
    Returns (snr, width, profile_of_best) with snr/width of shape (T,).
    """
    nsub, nbins = E.shape
    F = np.fft.rfft(E, axis=1)                                   # (nsub, nbins_f)
    k = np.arange(F.shape[1])
    Ntot = N.sum(axis=0)
    norm = 1.0 / np.sqrt(np.maximum(Ntot, 1.0))                  # unit-noise normalisation per bin
    T = len(shifts_turns)
    snr = np.empty(T); wid = np.empty(T, dtype=np.int64)
    best = (-np.inf, None)
    for b0 in range(0, T, batch):
        sh = shifts_turns[b0:b0 + batch]                         # (B, nsub)
        # the pulse of sub-integration j sits at +shift_j turns from its model phase: rotate the profile by
        # -shift_j (p_rot[b] = p[b + shift]), i.e. multiply the spectrum by exp(-2*pi*i*k*shift)
        phasor = np.exp(-2j * np.pi * k[None, None, :] * sh[:, :, None])     # (B, nsub, nbins_f)
        spec = np.einsum('bsk,sk->bk', phasor, F)
        prof = np.fft.irfft(spec, n=nbins, axis=1) * norm[None, :]
        s, w = boxcar_snr(prof.astype(np.float64), widths)
        snr[b0:b0 + len(s)] = s; wid[b0:b0 + len(s)] = w
        i = int(np.argmax(s))
        if s[i] > best[0]:
            best = (s[i], prof[i].copy())
    return snr, wid, best[1]


def choose_nsub(templates, f0, peak_template, tobs, nbins, max_drift_bins=0.5, nsub_min=32, nsub_max=1024,
                f0_halfrange_bins=0.0, ntest=300, ngrid=4096, rng=None):
    """
    Smallest power-of-two number of sub-integrations such that, for 99 % of (template, sub-integration) pairs,
    the phase drift inside one sub-integration stays below max_drift_bins profile bins. Each sub-integration
    is rotated by a single phase, so this bounds the smearing of the aligned profiles. Two drifts add up:
      - the orbital residual of the fine-grid templates relative to the origin (measured on the lattice),
      - the frequency offsets searched: f0_halfrange_bins / nsub turns per sub-integration.
    Returns (nsub, drift_99 in bins at that nsub).
    """
    rng = np.random.default_rng(0) if rng is None else rng
    sel = templates if len(templates) <= ntest else templates[rng.choice(len(templates), ntest, replace=False)]
    t = np.linspace(0, tobs, ngrid)
    shifts, _ = template_shifts(sel, f0, peak_template, t, 0.0)
    nsub = nsub_min
    while True:
        seg = shifts[:, :ngrid - ngrid % nsub].reshape(len(sel), nsub, -1)
        drift = (seg.max(axis=2) - seg.min(axis=2)).ravel() * nbins + f0_halfrange_bins * nbins / nsub
        d99 = float(np.percentile(drift, 99))
        if d99 <= max_drift_bins or nsub >= nsub_max:
            if d99 > max_drift_bins:
                logging.warning("nsub capped at %d: 99%% phase drift inside a sub-integration is %.2f bins "
                                "(limit %.2f); reduce search_halfrange_fac or raise nsub_max", nsub, d99, max_drift_bins)
            return nsub, d99
        nsub *= 2


def gpu_fold_profiles(resampler, dat_file, templates, f0_fold, nsub, nbins, mode='linear', workdir='fold_tmp'):
    """
    Resample and fold every template in one pass each with the resample_timeseries tool (--fold):
    template k is folded at frequency f0_fold[k]. Returns (profiles [T, nsub, nbins], counts [nsub, nbins]).
    """
    import subprocess, shutil
    os.makedirs(workdir, exist_ok=True)
    list_file = os.path.join(workdir, 'fold_list.txt'); out_file = os.path.join(workdir, 'profiles.bin')
    with open(list_file, 'w') as f:
        for t, f0 in zip(templates, f0_fold):
            f.write('%.17g %.17g %.17g %.17g\n' % (t[0], t[1], t[2], f0))
    cmd = [resampler, '-i', dat_file, '--fold', list_file, '--nsub', str(nsub), '--nbins', str(nbins), '--out', out_file, '--mode', mode]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        logging.error("resampler --fold failed: %s\n%s", ' '.join(cmd), res.stderr)
        sys.exit(1)
    d = np.fromfile(out_file, dtype=np.float32)
    T = len(templates)
    if len(d) != T * 2 * nsub * nbins:
        logging.error("unexpected profile file size %d for %d templates", len(d), T)
        sys.exit(1)
    d = d.reshape(T, 2, nsub, nbins).astype(np.float64)
    shutil.rmtree(workdir, ignore_errors=True)
    return d[:, 0], d[:, 1]            # profiles, noise variances per bin (both [T, nsub, nbins])


def gpu_fold_search(resampler, dat_file, templates, f0_fold, nsub, nbins, df_halfrange_bins, df_step_bins, ducy_max,
                    mode='nearest', workdir='fold_tmp'):
    """
    Resample, fold and search every template on the device (resample_timeseries --search): the folded profiles
    never leave the GPU; the tool returns per template the best (snr, boxcar width, frequency offset in Fourier
    bins) over the frequency-offset grid. Returns (snr [T], width [T], df_bins [T]).
    """
    import subprocess, shutil
    os.makedirs(workdir, exist_ok=True)
    list_file = os.path.join(workdir, 'fold_list.txt'); out_file = os.path.join(workdir, 'results.txt')
    with open(list_file, 'w') as f:
        for t, f0 in zip(templates, f0_fold):
            f.write('%.17g %.17g %.17g %.17g\n' % (t[0], t[1], t[2], f0))
    cmd = [resampler, '-i', dat_file, '--search', list_file, '--nsub', str(nsub), '--nbins', str(nbins),
           '--df_halfrange', '%g' % df_halfrange_bins, '--df_step', '%g' % df_step_bins, '--ducy_max', '%g' % ducy_max,
           '--out', out_file, '--mode', mode]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        logging.error("resampler --search failed: %s\n%s", ' '.join(cmd), res.stderr)
        sys.exit(1)
    r = np.loadtxt(out_file, ndmin=2)
    shutil.rmtree(workdir, ignore_errors=True)
    if len(r) != len(templates):
        logging.error("resampler --search returned %d results for %d templates", len(r), len(templates))
        sys.exit(1)
    return r[:, 0], r[:, 1].astype(int), r[:, 2]


def score_folded_profiles(profiles, counts, t_sub, dfreqs, widths, batch=1024):
    """
    Score GPU-folded profiles: for every template (exactly folded) and every frequency offset, rotate the
    sub-integrations by the linear phase ramp dfreq * t, sum, normalise and apply the boxcar filter.
    Returns (snr [T], width [T], dfreq [T]) of the best offset per template.
    """
    T, nsub, nbins = profiles.shape
    F = np.fft.rfft(profiles, axis=2)                                   # (T, nsub, nbins_f)
    k = np.arange(F.shape[2])
    # exact (Fourier) rotations preserve the noise: normalise by the summed per-bin variance of each template
    counts = np.asarray(counts)
    var = counts.sum(axis=1) if counts.ndim == 3 else np.broadcast_to(counts.sum(axis=0), (T, nbins))
    norm = 1.0 / np.sqrt(np.maximum(var, 1.0))                          # (T, nbins)
    best_snr = np.full(T, -np.inf); best_w = np.zeros(T, dtype=np.int64); best_df = np.zeros(T)
    for df in dfreqs:
        phasor = np.exp(-2j * np.pi * k[None, :] * (df * t_sub)[:, None])   # (nsub, nbins_f)
        for b0 in range(0, T, batch):
            spec = np.einsum('tsk,sk->tk', F[b0:b0 + batch], phasor)
            prof = np.fft.irfft(spec, n=nbins, axis=1) * norm[b0:b0 + batch]
            snr, w = boxcar_snr(prof, widths)
            sel = snr > best_snr[b0:b0 + batch]
            idx = np.flatnonzero(sel) + b0
            best_snr[idx] = snr[sel]; best_w[idx] = w[sel]; best_df[idx] = df
    return best_snr, best_w, best_df


def fold_search_peak(ts, tsamp, f0, peak_template, templates, nsub, nbins, dfreqs, widths, resampled=False):
    """
    Score every (template, dfreq) combination for one peak. Returns a dict with the best combination.
    'ts' is either the dedispersed series (folded here with the peak orbit) or, with resampled=True, the series
    already resampled into the peak orbit's emission frame (folded at constant f0).
    """
    if resampled:
        E, N, t_sub = fold_subints_resampled(ts, tsamp, f0, nsub, nbins)
    else:
        E, N, t_sub = fold_subints(ts, tsamp, f0, peak_template, nsub, nbins)
    shifts0, slope = template_shifts(templates, f0, peak_template, t_sub, 0.0)
    best = None
    for df in dfreqs:
        shifts = shifts0 + df * t_sub[None, :]
        snr, wid, prof = shift_add_score(E, N, shifts, widths)
        i = int(np.argmax(snr))
        if best is None or snr[i] > best['snr']:
            best = dict(snr=float(snr[i]), width=int(wid[i]), template=templates[i].copy(),
                        dfreq=float(df - slope[i]), dfreq_grid=float(df), itemplate=i)   # physical offset: f_true = f0 + df - slope
    # S/N of the peak's own template at zero frequency offset, for reference
    sh0, _ = template_shifts(peak_template[None, :], f0, peak_template, t_sub, 0.0)
    s0, _, _ = shift_add_score(E, N, sh0, widths)
    best['snr_peak_template'] = float(s0[0])
    return best


def write_peasoup_like_xml(reference_xml, output_xml, cands, ntemplates):
    """
    Write candidates in peasoup's XML layout, copying the header/search/segment sections of reference_xml so
    that the downstream fold scripts work unchanged. 'cands' is a list of dicts with keys
    period, dm, pb, a1, phi (deg), t0, snr and optional extras.
    """
    import uuid
    tree = ET.parse(reference_xml)
    root = tree.getroot()
    candidates = root.find('candidates')
    for child in list(candidates):
        candidates.remove(child)
    nt = root.find('search_parameters/ntemplates')
    if nt is not None:
        nt.text = str(int(ntemplates))
    tb = root.find('search_parameters/template_bank_technique')
    if tb is not None:
        tb.text = 'HIERARCHICAL_FOLD'
    cands = sorted(cands, key=lambda c: -c['snr'])
    for i, c in enumerate(cands):
        e = ET.SubElement(candidates, 'candidate', id=str(i))
        fields = [('period', '%.17g' % c['period']), ('opt_period', '0'), ('dm', '%.6f' % c['dm']), ('acc', '0'),
                  ('jerk', '0'), ('pb', '%.15g' % c['pb']), ('a1', '%.15g' % c['a1']), ('phi', '%.15g' % c['phi']),
                  ('t0', '%.10f' % c['t0']), ('omega', '0'), ('ecc', '0'), ('nh', '0'), ('snr', '%.6f' % c['snr']),
                  ('folded_snr', '%.6f' % c['snr']), ('is_adjacent', '0'), ('is_physical', '1'),
                  ('ddm_count_ratio', '1'), ('ddm_snr_ratio', '1'), ('nassoc', '0')]
        for key, val in fields:
            ET.SubElement(e, key).text = val
        for key, val in c.get('extra', {}).items():
            ET.SubElement(e, key).text = str(val)
        ET.SubElement(e, 'search_candidates_database_uuid').text = str(uuid.uuid4())
    tree.write(output_xml, encoding='utf-8', xml_declaration=True)


def cmd_foldsearch(args):
    """Stage 2 by folding: see the module docstring."""
    import csv
    peaks = list(csv.DictReader(open(args.peaks)))
    if args.only_used:
        peaks = [p for p in peaks if int(p.get('used', 1)) == 1]
    peaks = peaks[args.chunk::args.nchunks]
    logging.info("%d peaks in this job (chunk %d of %d)", len(peaks), args.chunk, args.nchunks)

    # time series per DM trial
    series = {}
    for dat in args.timeseries:
        data, info = read_presto_timeseries(dat)
        series[round(info['dm'], 4)] = (None if args.resampler else normalise_timeseries(data), info, dat)
        logging.info("%s: DM %.4f, %d samples, tsamp %.9g", dat, info['dm'], len(data), info['tsamp'])
    if args.resampler:
        logging.info("time series are resampled per peak with %s (%s)", args.resampler, args.resampler_mode)
    if not series:
        logging.error("no time series given"); sys.exit(1)
    dms = np.array(sorted(series))

    ref_info, _, _ = read_keplerian_xml(args.reference_xml)
    tsamp = ref_info['tsamp']
    bank_hdr = resolve_bank_parameters(args, args.reference_xml, 'fine grid')
    tobs = next(iter(series.values()))[1].get('nsamples', ref_info['segment_nsamples']) * tsamp
    if args.tobs is not None:
        tobs = args.tobs      # bank design length (or explicit); the series may be zero-padded beyond it
    pepoch = ref_info['segment_pepoch']
    halfrange = args.search_halfrange_fac * np.sqrt(args.nominal_mismatch)
    stepsize = args.search_stepsize_fac * np.sqrt(args.nominal_mismatch)
    freq_max = 1. / args.period_min
    widths = box_widths(args.nbins, args.ducy_max)
    dfreqs = np.arange(-args.f0_halfrange_bins, args.f0_halfrange_bins + 1e-9, args.f0_step_bins) / tobs

    # Per peak: fold frequency, orbital template, DM trials used
    peak_f0 = np.array([1. / float(p['period']) for p in peaks])
    peak_tmpl = np.array([[float(p['angular_velocity']), float(p['asini_c']), float(p['orb_phase_rad'])] for p in peaks]).reshape(-1, 3)
    peak_dms = [dms[np.argsort(np.abs(dms - float(p['dm'])))[:args.dm_trials]] for p in peaks]

    # Peaks sharing an orbital template (several harmonics / spin frequencies / DM trials of one stage-1 template)
    # share its fine lattice and, with the resampler, its resampled series: group by unique template.
    if len(peaks):
        uniq_tmpl, tmpl_of_peak = np.unique(peak_tmpl, axis=0, return_inverse=True)
        tmpl_of_peak = np.asarray(tmpl_of_peak).ravel()
    else:
        uniq_tmpl, tmpl_of_peak = np.zeros((0, 3)), np.zeros(0, dtype=int)
    logging.info("%d peaks, %d unique orbital templates", len(peaks), len(uniq_tmpl))

    lattices = {}
    def lattice_of(iu):
        if iu not in lattices:
            centers = uniq_tmpl[iu][None, :]
            if args.lattice == 'adaptive':
                lattices[iu], _ = advancing_front_lattice(centers, lambda t: circular_metric(t, freq_max, tobs, args.order_highest),
                                                          lambda t: orbital_an(t, freq_max, tobs, args.order_highest), halfrange, stepsize)
            else:
                lattices[iu], _ = eigen_lattice(centers, circular_metric(centers, freq_max, tobs, args.order_highest), halfrange, stepsize)
            if not args.no_bank_clip and bank_hdr:
                n0 = len(lattices[iu])
                lattices[iu], _ = clip_to_bank_region(lattices[iu], np.zeros(n0, dtype=int), bank_hdr, centers)
                logging.debug("template %d: %d of %d lattice templates inside the bank region", iu, len(lattices[iu]), n0)
        return lattices[iu]

    best_of_peak = [None] * len(peaks)
    nsub_of_peak = [0] * len(peaks)
    ntemplates_total = 0

    def search_peak(ip, ts, dm):
        """Score the lattice of peak ip on the series ts (already in the frame of its template if resampling)."""
        templates = lattice_of(tmpl_of_peak[ip])
        if args.nsub > 0:
            nsub, drift = args.nsub, float('nan')
        else:
            nsub, drift = choose_nsub(templates, peak_f0[ip], peak_tmpl[ip], tobs, args.nbins, args.max_phase_drift_bins,
                                      nsub_max=args.nsub_max, f0_halfrange_bins=args.f0_halfrange_bins)
        res = fold_search_peak(ts, tsamp, peak_f0[ip], peak_tmpl[ip], templates, nsub, args.nbins, dfreqs, widths,
                               resampled=bool(args.resampler))
        res['dm'] = float(dm); res['nsub'] = nsub; res['drift'] = drift
        if best_of_peak[ip] is None or res['snr'] > best_of_peak[ip]['snr']:
            best_of_peak[ip] = res
        return len(templates) * len(dfreqs)

    def search_peak_gpu_fold(ip, dat, dm):
        """
        Exact variant: every lattice template is resampled and folded by the tool at its own frequency
        f_obs - c1(template) (c1 = linear Doppler term of the template relative to the peak), so that only the
        frequency grid remains to be searched on the folded profiles. No orbital phase approximation.
        """
        templates = lattice_of(tmpl_of_peak[ip])
        f0 = peak_f0[ip]
        t_fine = np.linspace(0, tobs, 1024)
        _, slope = template_shifts(templates, f0, peak_tmpl[ip], t_fine, 0.0)
        f0_fold = f0 - slope
        nsub = args.nsub if args.nsub > 0 else int(2 ** np.ceil(np.log2(max(32, args.nbins * args.f0_halfrange_bins / args.max_phase_drift_bins))))
        snr, wid, df_bins = gpu_fold_search(args.resampler, dat, templates, f0_fold, nsub, args.nbins,
                                            args.f0_halfrange_bins, args.f0_step_bins, args.ducy_max, args.resampler_mode)
        df = df_bins / tobs
        i = int(np.argmax(snr))
        ipk = int(np.argmin(np.linalg.norm(templates - peak_tmpl[ip], axis=1)))   # the peak's own template
        res = dict(snr=float(snr[i]), width=int(wid[i]), template=templates[i].copy(),
                   dfreq=float(f0_fold[i] + df[i] - f0), dfreq_grid=float(df[i]), itemplate=i,
                   snr_peak_template=float(snr[ipk]), dm=float(dm), nsub=nsub, drift=float(args.nbins * args.f0_halfrange_bins / nsub))
        if best_of_peak[ip] is None or res['snr'] > best_of_peak[ip]['snr']:
            best_of_peak[ip] = res
        return len(templates)

    # Work list: (DM trial, unique template) -> peaks
    work = {}
    for ip in range(len(peaks)):
        for dm in peak_dms[ip]:
            work.setdefault((round(float(dm), 4), int(tmpl_of_peak[ip])), []).append(ip)

    if args.resampler and args.gpu_fold:
        nfold = 0
        for (dm, iu), ips in work.items():
            for ip in ips:
                n = search_peak_gpu_fold(ip, series[dm][2], dm)
                nfold += n
                ntemplates_total += n * len(dfreqs)
        logging.info("%d templates resampled and folded on the device for %d peaks", nfold, len(peaks))
    elif args.resampler:
        import shutil
        nres = 0
        for dm in sorted({k[0] for k in work}):
            dat = series[dm][2]
            tmpl_ids = sorted({k[1] for k in work if k[0] == dm})
            for b0 in range(0, len(tmpl_ids), args.resample_batch):
                batch = tmpl_ids[b0:b0 + args.resample_batch]
                files = resample_batch_with_tool(args.resampler, dat, uniq_tmpl[batch], args.resampler_mode)
                nres += len(batch)
                for iu, f in zip(batch, files):
                    ts = normalise_timeseries(np.fromfile(f, dtype=np.float32))
                    for ip in work[(dm, iu)]:
                        ntemplates_total += search_peak(ip, ts, dm)
                shutil.rmtree('resampled_tmp', ignore_errors=True)
        logging.info("%d resampling calls (unique template x DM trial) for %d peaks", nres, len(peaks))
    else:
        for (dm, iu), ips in work.items():
            ts = series[dm][0]
            for ip in ips:
                ntemplates_total += search_peak(ip, ts, dm)

    cands = []
    for ip, p in enumerate(peaks):
        best = best_of_peak[ip]
        f0 = peak_f0[ip]
        tmpl = best['template']
        pb_days = 2 * np.pi / tmpl[0] / 86400.
        phi_deg = np.rad2deg(tmpl[2] % (2 * np.pi))
        f0_new = f0 + best['dfreq']
        cands.append(dict(period=1. / f0_new, dm=best['dm'], pb=pb_days, a1=tmpl[1], phi=phi_deg,
                          t0=pepoch + (phi_deg / 360.) * pb_days, snr=best['snr'],
                          extra=dict(fold_width_bins=best['width'], fold_dfreq=best['dfreq'],
                                     fold_snr_peak_template=best['snr_peak_template'], stage1_snr=p['snr'],
                                     stage1_period=p['period'], fold_nsub=best['nsub'])))
        logging.info("peak %d/%d: f0 %.6f stage-1 S/N %s -> fold S/N %.2f (peak template %.2f), width %d bins, df %.2e Hz, DM %.3f, nsub %d (99%% drift %.2f bins)",
                     ip + 1, len(peaks), f0, p['snr'], best['snr'], best['snr_peak_template'], best['width'], best['dfreq'], best['dm'], best['nsub'], best['drift'])
    write_peasoup_like_xml(args.reference_xml, args.output, cands, ntemplates_total)
    logging.info("wrote %s with %d candidates (%d template evaluations)", args.output, len(cands), ntemplates_total)


###############################################################################
# CLI
###############################################################################

def add_cluster_options(p):
    p.add_argument('--order_highest', type=int, default=10, help='highest Legendre order (default 10)')
    p.add_argument('--f0tol_fac', type=float, default=0.001, help='down-weighting of the spin frequency axis (default 0.001)')
    p.add_argument('--nominal_mismatch', type=float, default=None,
                   help='nominal mismatch of the template bank (default: MISMATCH from the bank header)')
    p.add_argument('--template_bank', default=None,
                   help='template bank file whose header provides the defaults (default: the bank recorded in the XML)')
    p.add_argument('--distance_tol', type=float, default=50.0,
                   help='clustering radius in units of sqrt(nominal_mismatch) (default 50)')
    p.add_argument('--min_prominence', type=float, default=0.0,
                   help='absorb a peak into a brighter touching cluster if it rises less than this '
                        'S/N above the saddle between them (default 0 = keep every local maximum)')
    p.add_argument('--tobs', type=float, default=None,
                   help='observation length in seconds used for a_n and the metric (default: TOBS from the bank header, '
                        'else the segment length from the XML)')
    p.add_argument('--workers', type=int, default=1, help='threads for the neighbour search (-1 = all cores)')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-v', '--verbose', action='store_true')
    sub = parser.add_subparsers(dest='command', required=True)

    pc = sub.add_parser('cluster', help='cluster stage-1 candidates and write fine template bank chunks')
    pc.add_argument('-i', '--input', nargs='+', required=True, help='stage-1 peasoup XML files')
    pc.add_argument('-p', '--prefix', default='fine_tb', help='template bank chunk prefix (default fine_tb)')
    pc.add_argument('--peaks_file', default='clustered_peaks.csv')
    pc.add_argument('--summary_file', default='hierarchical_summary.json')
    add_cluster_options(pc)
    pc.add_argument('--min_snr', type=float, default=None, help='optional S/N cut on stage-1 candidates')
    pc.add_argument('--search_halfrange_fac', type=float, default=0.6,
                    help='half extent of the fine lattice in units of sqrt(nominal_mismatch) (default 0.6)')
    pc.add_argument('--search_stepsize_fac', type=float, default=0.2,
                    help='lattice step in units of sqrt(nominal_mismatch) (default 0.2)')
    pc.add_argument('--period_min', type=float, default=None,
                    help='shortest spin period (s) at which the metric is evaluated (default: SPIN PERIOD from the bank header)')
    pc.add_argument('--no_bank_clip', action='store_true',
                    help='do not drop fine-grid templates outside the physical region of the bank (PORB, mass limits)')
    pc.add_argument('--lattice', choices=['linear', 'adaptive'], default='linear',
                    help='linear: lattice from the metric at the peak; adaptive: advancing front that re-evaluates '
                         'the metric layer by layer and measures every distance in Legendre-coefficient space')
    pc.add_argument('--max_peaks', type=int, default=None,
                    help='keep at most this many peaks (the brightest) for the fine grid')
    pc.add_argument('--max_templates', type=int, default=None,
                    help='keep at most this many fine-grid templates (whole peaks, brightest first)')
    pc.add_argument('--templates_per_chunk', type=int, default=1000000)
    pc.add_argument('--no_bank_files', action='store_true',
                    help='do not write the template bank chunk files (stage 2 by folding regenerates the lattice per peak)')
    pc.set_defaults(func=cmd_cluster)

    pm = sub.add_parser('merge', help='merge stage-2 chunk XML files into one XML file')
    pm.add_argument('-i', '--input', nargs='+', required=True, help='stage-2 peasoup XML files')
    pm.add_argument('-o', '--output', default='output.xml')
    pm.add_argument('--cluster', action='store_true', help='keep only S/N peaks in a_n space')
    add_cluster_options(pm)
    pm.set_defaults(func=cmd_merge)

    pf = sub.add_parser('foldsearch', help='stage 2 by folding the dumped time series into sub-integrations and aligning '
                                           'them per fine-grid template (LOKI scheme)')
    pf.add_argument('--peaks', required=True, help='clustered_peaks.csv from the cluster step')
    pf.add_argument('--timeseries', nargs='+', required=True, help='peasoup -d .dat files (with .inf) of the DM trials')
    pf.add_argument('--reference_xml', required=True, help='a stage-1 output.xml of the same segment (header template)')
    pf.add_argument('-o', '--output', default='output.xml')
    pf.add_argument('--chunk', type=int, default=0, help='this job processes peaks chunk::nchunks')
    pf.add_argument('--nchunks', type=int, default=1)
    pf.add_argument('--only_used', action='store_true', help='skip peaks dropped by max_peaks / max_templates')
    pf.add_argument('--resampler', default=None,
                    help='path of the resample_timeseries tool (peasoup GPU kernels); when given, the series is '
                         'resampled into the emission frame of each peak orbit before folding, otherwise it is '
                         'folded with the orbit model directly')
    pf.add_argument('--gpu_fold', action='store_true',
                    help='with --resampler: resample and fold every fine-grid template on the device (exact, no '
                         'sub-integration rotation for the orbit); otherwise resample only the peak template and '
                         'search the lattice by rotating the sub-integration profiles')
    pf.add_argument('--resampler_mode', choices=['nearest', 'linear', 'exact'], default='nearest',
                    help='nearest (default): peasoup default, as stage 1, independent noise per sample; linear: '
                         'interpolation at the fractional index (correlates neighbouring samples and biases the '
                         'boxcar S/N when nbins exceeds the samples per period); exact: peasoup --exact_resampler '
                         'path (shifted time origin)')
    pf.add_argument('--resample_batch', type=int, default=16,
                    help='templates per resampler call (the series stays on the GPU for the batch; default 16)')
    pf.add_argument('--nsub', type=int, default=0,
                    help='number of sub-integrations; 0 (default) chooses per peak the smallest power of two for which '
                         'the residual phase of 99%% of the (template, sub-integration) pairs drifts by less than '
                         '--max_phase_drift_bins inside one sub-integration')
    pf.add_argument('--max_phase_drift_bins', type=float, default=0.5)
    pf.add_argument('--nsub_max', type=int, default=1024)
    pf.add_argument('--nbins', type=int, default=64, help='profile bins (default 64)')
    pf.add_argument('--ducy_max', type=float, default=0.3, help='largest boxcar width as a fraction of the profile')
    pf.add_argument('--f0_halfrange_bins', type=float, default=0.6,
                    help='frequency offsets searched around every template, in Fourier bins (1/tobs); the linear '
                         'phase term of each template is removed analytically, so this only has to cover the FFT '
                         'quantisation of the stage-1 frequency (default 0.6)')
    pf.add_argument('--f0_step_bins', type=float, default=0.05,
                    help='step of the frequency offsets in Fourier bins; must be a fraction of the narrowest pulse '
                         'width in turns, i.e. about 0.5 * width_bins / nbins (default 0.05)')
    pf.add_argument('--dm_trials', type=int, default=1, help='number of nearest DM trials folded per peak')
    pf.add_argument('--order_highest', type=int, default=10)
    pf.add_argument('--nominal_mismatch', type=float, default=None)
    pf.add_argument('--template_bank', default=None)
    pf.add_argument('--search_halfrange_fac', type=float, default=0.6)
    pf.add_argument('--search_stepsize_fac', type=float, default=0.2)
    pf.add_argument('--period_min', type=float, default=None)
    pf.add_argument('--tobs', type=float, default=None,
                    help='observation length (s) for a_n, the metric and the df grid (default: TOBS of the bank, else the series length)')
    pf.add_argument('--no_bank_clip', action='store_true')
    pf.add_argument('--lattice', choices=['linear', 'adaptive'], default='linear')
    pf.set_defaults(func=cmd_foldsearch)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        stream=sys.stdout)
    args.func(args)


if __name__ == '__main__':
    main()
