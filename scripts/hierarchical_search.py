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
import scipy.linalg
import scipy.spatial
import scipy.special


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
                                    - np.sqrt(3) * vr * scipy.special.spherical_jn(1, vs) * np.cos(vp))
    for n in range(2, order_highest + 1):
        coordinate[:, n - 1] = -np.sqrt(2 * n + 1) * vr * scipy.special.spherical_jn(n, vs) * np.sin(vp + n * np.pi / 2.)
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
        jn = scipy.special.spherical_jn(n, s)
        djn = scipy.special.spherical_jn(n, s, derivative=True)
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
    metrics = circular_metric(centers, 1. / args.period_min, tobs, args.order_highest)
    templates, owner = eigen_lattice(centers, metrics, halfrange, stepsize)
    logging.info("%d fine-grid templates generated (%.1f per peak)", len(templates), len(templates) / max(1, len(centers)))

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

    nchunks = max(1, int(np.ceil(len(templates) / float(args.templates_per_chunk))))
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
                        min_snr=args.min_snr, min_prominence=args.min_prominence,
                        max_peaks=args.max_peaks, max_templates=args.max_templates,
                        templates_per_chunk=args.templates_per_chunk),
    )
    with open(args.summary_file, 'w') as f:
        json.dump(summary, f, indent=2)


def cmd_merge(args):
    infos, arrays, trees = read_many(args.input)
    ncands_total = len(arrays['snr'])
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
# CLI
###############################################################################

def add_cluster_options(p):
    p.add_argument('--order_highest', type=int, default=10, help='highest Legendre order (default 10)')
    p.add_argument('--f0tol_fac', type=float, default=0.001, help='down-weighting of the spin frequency axis (default 0.001)')
    p.add_argument('--nominal_mismatch', type=float, default=0.15, help='nominal mismatch of the template bank (default 0.15)')
    p.add_argument('--distance_tol', type=float, default=50.0,
                   help='clustering radius in units of sqrt(nominal_mismatch) (default 50)')
    p.add_argument('--min_prominence', type=float, default=0.0,
                   help='absorb a peak into a brighter touching cluster if it rises less than this '
                        'S/N above the saddle between them (default 0 = keep every local maximum)')
    p.add_argument('--tobs', type=float, default=None,
                   help='observation length in seconds used for a_n and the metric (default: segment length from the XML)')
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
    pc.add_argument('--period_min', type=float, default=0.0025,
                    help='shortest spin period (s) at which the metric is evaluated (default 2.5 ms)')
    pc.add_argument('--max_peaks', type=int, default=None,
                    help='keep at most this many peaks (the brightest) for the fine grid')
    pc.add_argument('--max_templates', type=int, default=None,
                    help='keep at most this many fine-grid templates (whole peaks, brightest first)')
    pc.add_argument('--templates_per_chunk', type=int, default=1000000)
    pc.set_defaults(func=cmd_cluster)

    pm = sub.add_parser('merge', help='merge stage-2 chunk XML files into one XML file')
    pm.add_argument('-i', '--input', nargs='+', required=True, help='stage-2 peasoup XML files')
    pm.add_argument('-o', '--output', default='output.xml')
    pm.add_argument('--cluster', action='store_true', help='keep only S/N peaks in a_n space')
    add_cluster_options(pm)
    pm.set_defaults(func=cmd_merge)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        stream=sys.stdout)
    args.func(args)


if __name__ == '__main__':
    main()
