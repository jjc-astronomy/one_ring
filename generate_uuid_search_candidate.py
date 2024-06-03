import argparse
import xml.etree.ElementTree as ET
import uuid_utils

def process_xml_file(input_file, output_file):
    # Parse the XML file
    tree = ET.parse(input_file)
    root = tree.getroot()
    candidates = root.find(".//candidates")

    # Process each candidate and add a database_uuid
    for candidate in candidates.findall("candidate"):
        uuid_str = uuid_utils.generate_uuid_string()
        uuid_element = ET.Element("search_candidates_database_uuid")
        uuid_element.text = uuid_str
        candidate.append(uuid_element)

    # Write to the output file
    tree.write(output_file, encoding="utf-8", xml_declaration=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process an XML file and add UUIDs to candidates.")
    parser.add_argument('-i', '--input', required=True, help="Input XML file")
    parser.add_argument('-o', '--output', default='output.xml', help="Output XML file (default: output.xml)")

    args = parser.parse_args()

    process_xml_file(args.input, args.output)


