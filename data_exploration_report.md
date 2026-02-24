# Data Exploration and Cleaning Report

## 1. Dataset Overview

The primary dataset for this project is the English Wikipedia stub-meta-history dump, obtained from [Wikimedia Downloads](https://dumps.wikimedia.org/enwiki/latest/). This dump consists of 27 compressed XML files (`enwiki-latest-stub-meta-history1.xml.gz` through `enwiki-latest-stub-meta-history27.xml.gz`) totaling approximately 100 GB compressed and an estimated 750--1,000 GB uncompressed. Each file contains the complete revision metadata for a range of Wikipedia pages, including every edit ever made to those pages.

The stub-meta-history dump records **revision metadata only** -- it does not contain the actual article text. For each revision, it stores who made the edit, when, the edit summary, whether it was flagged as a minor edit, the byte size of the article after the edit, and a SHA1 hash of the content. This is sufficient for analyzing revision frequency and detecting vandalism patterns without requiring the full multi-terabyte article text dump.

## 2. Data Exploration

### 2.1 Understanding the XML Structure

The first step was writing an exploration script (`explore_stub_history.py`) to understand the XML schema by streaming through the first few pages of a decompressed file. The dump follows the MediaWiki XML export schema (version 0.11) and has this hierarchical structure:

- **`<mediawiki>`** -- root element with schema version and language attributes
  - **`<siteinfo>`** -- dataset metadata: site name ("Wikipedia"), database name ("enwiki"), generator version, and a list of 30 namespace definitions (Main, Talk, User, File, Template, Category, etc.)
  - **`<page>`** (repeated, one per article) -- contains:
    - `<title>` -- article title
    - `<ns>` -- namespace key (0 = main article namespace)
    - `<id>` -- unique page ID
    - `<redirect>` -- optional, present if the page is a redirect, with a `title` attribute for the target
    - **`<revision>`** (repeated, one per edit) -- contains:
      - `<id>` -- unique revision ID
      - `<parentid>` -- ID of the previous revision (forms edit chains)
      - `<timestamp>` -- ISO 8601 timestamp of the edit
      - `<contributor>` -- either `<username>` + `<id>` for registered users, or `<ip>` for anonymous editors
      - `<comment>` -- edit summary written by the editor
      - `<minor/>` -- flag indicating a minor edit
      - `<text>` -- stub element with no body text, but `bytes` (article size) and `sha1` (content hash) attributes
      - `<sha1>` -- content hash

A single page can have thousands of revisions. For example, the "Anarchism" article in file 1 alone has 20,330 revisions. The 27 dump files are split by page ID ranges, meaning any given politician's page appears in exactly one file, but politicians are scattered across all 27 files.

### 2.2 Building the Politician Filter List

Since the dump contains all of English Wikipedia (millions of articles), the next step was to identify which pages correspond to US politicians and US political parties. The Wikidata SPARQL endpoint (`query.wikidata.org/sparql`) was queried to build this list using four complementary queries:

1. **US politicians by occupation** -- humans with US citizenship (P27=Q30) and occupation "politician" (P106=Q82955) or "political candidate" (P106=Q13231463)
2. **US politicians by direct position** -- humans who held specific offices: President, Vice President, US Senator, US Representative, state governor, lieutenant governor, Secretary of State, or mayor
3. **US politicians by position subclass** -- a broader query traversing subclass hierarchies of US government positions
4. **US political parties** -- entities that are instances or subclasses of "political party" (Q7278) with country US (P17=Q30)

The union of these queries produced **75,540 unique English Wikipedia article titles** (74,941 politicians and 599 political parties). Spot-checking confirmed that all major expected names were present: Barack Obama, Donald Trump, Joe Biden, Abraham Lincoln, George Washington, Nancy Pelosi, Alexandria Ocasio-Cortez, Democratic Party, Republican Party, and others.

### 2.3 Filtering the XML Dumps

A streaming filter script (`filter_xml.py`) was built to process each XML dump file using Python's `xml.etree.ElementTree.iterparse`. The script:

1. Loads the 75,540 politician titles into a Python `set` for O(1) lookup
2. Streams through the XML, reading one `<page>` element at a time
3. Checks each page's `<title>` against the title set
4. For matches, extracts all revision children into flat rows with 14 fields
5. For non-matches, immediately clears the element from memory
6. Writes matched data to Parquet files

The script reads `.xml.gz` files directly via Python's `gzip` module, decompressing on the fly without writing uncompressed data to disk. This was essential since the uncompressed files are 25--50 GB each and there was insufficient disk space to decompress all 27 simultaneously.

Files are processed one at a time to keep resource usage low, only using about ~100 MB of RAM regardless of input file size.

### 2.4 Exploratory Analysis of Extracted Data

After filtering one compressed file as a validation run, the extracted Parquet data was analyzed to confirm it contained sufficient information for the project goals:

- **Revision frequency over time**: The `timestamp` field allows binning revisions by time. A test on the Abraham Lincoln article revealed clear temporal patterns -- heavy editing in 2005--2006, a resurgence in 2020 and 2025 -- consistent with known periods of public interest.
- **Vandalism detection signals**: Multiple independent signals were identified in the data:
  - **Edit summary keywords**: 10.67% of comments contain "revert", 2.90% mention "vandal" explicitly, 3.10% contain "undid", and 0.64% use the shorthand "rvv" (revert vandalism)
  - **Content hash (SHA1)**: When the same `text_sha1` appears in multiple revisions of the same page, the article was reverted to a prior state. Abraham Lincoln has 1,571 SHA1 values that recur, some appearing 30 times.
  - **Article size changes (text_bytes)**: Vandalism often manifests as a sudden large drop (blanking) or spike (spam) in byte count, immediately followed by restoration. The byte delta between consecutive revisions is computable from `text_bytes` and `parent_id`.
  - **Contributor type**: 27.6% of all edits are anonymous (IP-only), which correlates with higher vandalism rates.
  - **Minor edit flag**: 26.1% of edits are flagged minor; vandalism appears to almost never be flagged as such.

### 2.5 Coverage Validation

After processing all 27 files, I found that:

- **75,503 out of 75,540** expected titles were found (99.95% coverage)
- Only **37 titles** were missing -- all are very obscure politicians whose Wikipedia articles likely existed in Wikidata but were deleted or renamed on Wikipedia since the Wikidata entry was created
- **9,541,529 total revision rows** were extracted across all politician pages

## 3. Data Parsing and Quality Issues

### 3.1 XML Namespace Handling

**Issue**: All XML element tags in the dump are prefixed with the MediaWiki namespace URI (`http://www.mediawiki.org/xml/export-0.11/`), so a tag like `<title>` is internally represented as `{http://www.mediawiki.org/xml/export-0.11/}title`. Searching for elements by bare tag name silently returns no results.

**Resolution**: A `strip_ns()` helper function was written to remove the namespace prefix from tags during iteration, and an `ns_tag()` function was used to prepend the namespace when calling `elem.find()`. This ensured correct element matching throughout the parser.

### 3.2 Memory Management for Multi-Gigabyte Files

**Issue**: Loading even a single 25 GB XML file into memory via `ET.parse()` is infeasible. The data would require hundreds of gigabytes of RAM due to DOM overhead.

**Resolution**: The `ET.iterparse()` streaming API was used, which processes elements one at a time. After each `<page>` element is processed (whether matched or not), `elem.clear()` is called immediately to release memory. This keeps total memory usage under ~100 MB regardless of file size.


### 3.3 Output Buffering in Background Execution

**Issue**: When the filter script was run as a background process, Python's stdout was fully buffered (since it was not connected to a TTY). No progress output was visible for several minutes even though the script was actively processing data, making it impossible to monitor progress.

**Resolution**: Two mitigations were applied: (1) the Python `-u` flag was used to force unbuffered stdout, and (2) a global `print()` override was added to the script that sets `flush=True` on every call. Together, these ensure real-time progress output regardless of how the script is invoked.

### 3.4 Compressed File I/O and Disk Space Constraints

**Issue**: The 27 dump files total ~100 GB compressed and ~750--1,000 GB uncompressed. There was insufficient disk space to decompress all files simultaneously, and decompressing-then-processing would require significant temporary storage.

**Resolution**: The filter script was designed to read `.xml.gz` files directly using Python's `gzip.open()`, which decompresses the data as a stream without writing anything to disk. The only disk output is the small Parquet files (~20--80 MB each). This reduced disk space requirements from hundreds of gigabytes to under 1 GB for the full output.

### 3.5 Null and Missing Value Patterns

**Issue**: Several columns in the extracted data have structurally expected null patterns that could be mistaken for data quality problems:

| Column | Null Rate | Reason |
|---|---|---|
| `contributor_username` / `contributor_id` | 19.2% | Anonymous edits have only an IP address, no username |
| `contributor_ip` | 80.8% | Registered user edits have only a username, no IP |
| `comment` | 19.8% | Editors are not required to provide an edit summary |
| `parent_id` | 0.8% | The very first revision of each page has no parent |
| `redirect_target` | 99.9% | Only redirect pages have a target; most pages are not redirects |

**Resolution**: These null patterns are inherent to the Wikipedia data model, not data corruption. The Parquet schema uses nullable types for these columns. Future analysis code must account for these patterns -- for example, checking `contributor_ip IS NOT NULL` to identify anonymous edits rather than assuming a missing username indicates bad data.

### 3.6 Non-US Politicians in Wikidata Results

**Issue**: The Wikidata SPARQL query for "occupation = politician AND citizenship = US" returned some clearly non-US figures such as Roman emperors (Augustus, Caligula, Claudius, Constantine the Great) and a Hungarian composer (Bela Bartok). These appear to have incorrect or overly broad Wikidata property assignments.

**Resolution**: These false positives represent a negligible fraction of the 75,540 titles (~0.02%) and will be filtered out during the analysis phase. The trade-off of casting a slightly wider net was preferable to the risk of excluding legitimate US politicians through overly restrictive queries.

## 4. Final Dataset Summary

| Metric | Value |
|---|---|
| Original data size (compressed) | ~100 GB across 27 XML files |
| Original data size (uncompressed, estimated) | ~750--1,000 GB |
| Politician title list | 75,540 titles from Wikidata |
| Titles found in dumps | 75,503 (99.95% coverage) |
| Total revision rows extracted | 9,541,529 |
| Final combined Parquet file size | 757.2 MB |
| Data reduction ratio | ~1,300:1 vs. uncompressed XML |

The output schema provides 14 fields per revision: `page_id`, `page_title`, `is_redirect`, `redirect_target`, `revision_id`, `parent_id`, `timestamp`, `contributor_username`, `contributor_id`, `contributor_ip`, `comment`, `is_minor`, `text_bytes`, and `text_sha1`. This dataset is ready for the analysis phase: studying vandalism frequency by politician, correlating revision spikes with news events, and comparing vandalism rates across political parties.
