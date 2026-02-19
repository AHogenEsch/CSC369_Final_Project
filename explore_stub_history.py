"""
explore_stub_history.py

Opens a Wikipedia stub-meta-history XML dump and prints a structured
summary of the data it contains:
  1. Top-level <mediawiki> attributes (schema version, language, etc.)
  2. <siteinfo> metadata (site name, database, generator, namespaces)
  3. Structure of the first few <page> elements and their <revision> children

Uses iterative XML parsing (iterparse) so it can handle multi-GB files
without loading everything into memory.
"""

import xml.etree.ElementTree as ET
import os
import sys
from collections import OrderedDict

# ── Configuration ────────────────────────────────────────────────────────
XML_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "enwiki-latest-stub-meta-history1.xml",
    "enwiki-latest-stub-meta-history1.xml",
)

# How many <page> elements to inspect before stopping
MAX_PAGES = 3
# How many <revision> elements per page to inspect
MAX_REVISIONS_PER_PAGE = 3

# Wikipedia dump XML namespace
NS = "http://www.mediawiki.org/xml/export-0.11/"


def strip_ns(tag: str) -> str:
    """Remove the XML namespace prefix from a tag name."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def print_section(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def print_element_summary(elem, indent=0):
    """Print tag, attributes, and text of an element (non-recursive)."""
    prefix = "  " * indent
    tag = strip_ns(elem.tag)
    attrs = ", ".join(f'{k}="{v}"' for k, v in elem.attrib.items())
    text = (elem.text or "").strip()

    parts = [f"{prefix}<{tag}>"]
    if attrs:
        parts[0] = f"{prefix}<{tag} {attrs}>"
    if text:
        parts.append(f" = \"{text}\"")
    print("".join(parts))


def explore_siteinfo(siteinfo_elem):
    """Pretty-print the <siteinfo> block."""
    print_section("SITEINFO (dataset metadata)")
    for child in siteinfo_elem:
        tag = strip_ns(child.tag)
        if tag == "namespaces":
            print(f"\n  Namespaces ({len(child)} entries):")
            for ns in child:
                key = ns.attrib.get("key", "?")
                case = ns.attrib.get("case", "?")
                name = (ns.text or "(main/article)").strip()
                print(f"    key={key:>5}  case={case:<14}  name={name}")
        else:
            text = (child.text or "").strip()
            print(f"  {tag}: {text}")


def explore_page(page_elem, page_num):
    """Pretty-print the structure of a single <page> element."""
    print_section(f"PAGE #{page_num}")

    revision_count = 0
    for child in page_elem:
        tag = strip_ns(child.tag)

        if tag == "revision":
            revision_count += 1
            if revision_count > MAX_REVISIONS_PER_PAGE:
                continue

            print(f"\n  -- Revision #{revision_count} --")
            for rev_child in child:
                rev_tag = strip_ns(rev_child.tag)

                if rev_tag == "contributor":
                    # Contributor can have <username>+<id> or <ip>
                    for contrib_child in rev_child:
                        ctag = strip_ns(contrib_child.tag)
                        ctext = (contrib_child.text or "").strip()
                        print(f"    contributor.{ctag}: {ctext}")
                elif rev_tag == "text":
                    # <text> in stubs has no body, just attributes
                    attrs = dict(rev_child.attrib)
                    print(f"    {rev_tag}: (stub – no body text)")
                    for k, v in attrs.items():
                        print(f"      @{k} = {v}")
                else:
                    text = (rev_child.text or "").strip()
                    attrs_str = " ".join(
                        f'{k}="{v}"' for k, v in rev_child.attrib.items()
                    )
                    line = f"    {rev_tag}: {text}"
                    if attrs_str:
                        line += f"  [{attrs_str}]"
                    print(line)
        else:
            text = (child.text or "").strip()
            attrs_str = " ".join(
                f'{k}="{v}"' for k, v in child.attrib.items()
            )
            line = f"  {tag}: {text}"
            if attrs_str:
                line += f"  [{attrs_str}]"
            print(line)

    if revision_count > MAX_REVISIONS_PER_PAGE:
        print(
            f"\n  ... ({revision_count} total revisions, "
            f"showed first {MAX_REVISIONS_PER_PAGE})"
        )
    else:
        print(f"\n  Total revisions in this page: {revision_count}")


def main():
    if not os.path.exists(XML_PATH):
        print(f"ERROR: File not found:\n  {XML_PATH}")
        sys.exit(1)

    file_size_gb = os.path.getsize(XML_PATH) / (1024 ** 3)
    print(f"File: {XML_PATH}")
    print(f"Size: {file_size_gb:.2f} GB")

    # ── 1. Read root attributes ─────────────────────────────────────────
    print_section("ROOT <mediawiki> ATTRIBUTES")
    # Quick pass: grab just the opening tag attributes
    for event, elem in ET.iterparse(XML_PATH, events=("start",)):
        root_tag = strip_ns(elem.tag)
        if root_tag == "mediawiki":
            for k, v in elem.attrib.items():
                print(f"  {strip_ns(k)}: {v}")
            break

    # ── 2. Stream through siteinfo + first N pages ──────────────────────
    pages_seen = 0
    context = ET.iterparse(XML_PATH, events=("end",))

    for event, elem in context:
        tag = strip_ns(elem.tag)

        if tag == "siteinfo":
            explore_siteinfo(elem)
            elem.clear()

        elif tag == "page":
            pages_seen += 1
            explore_page(elem, pages_seen)
            elem.clear()

            if pages_seen >= MAX_PAGES:
                print_section("SUMMARY")
                print(
                    f"  Explored {MAX_PAGES} page(s). The file likely "
                    f"contains many more.\n"
                    f"  Each <page> has metadata (title, ns, id) and one or\n"
                    f"  more <revision> entries with timestamp, contributor,\n"
                    f"  comment, sha1, and a stub <text> element (no body)."
                )
                break

    del context


if __name__ == "__main__":
    main()
