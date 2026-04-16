"""
Power BI PBIP (Power BI Project) Connector
Provides file-based editing for PBIP format to safely rename tables, columns, measures
without breaking report visuals.

Supports BOTH report formats:
  - PBIR-Legacy: Single report.json file (older format)
  - PBIR: Individual visual.json files (new enhanced format, default from Jan 2026)

PBIP Structure:
  project.pbip
  ProjectName.SemanticModel/
    definition.tmdl (or model.tmd)
    definition/
      tables/*.tmdl
      relationships/*.tmdl  <- Individual relationship files
      relationships.tmdl    <- Or single file
      cultures/*.tmdl       <- Linguistic schema (ConceptualEntity references)
      etc.
  ProjectName.Report/
    report.json             <- PBIR-Legacy: All visuals in one file
    definition.pbir         <- Points to semantic model
    definition/             <- PBIR Enhanced format (new)
      report.json           <- Report-level settings only
      pages/
        pages.json          <- Page listing
        [page_id]/
          page.json         <- Page settings
          visuals/
            [visual_id]/
              visual.json   <- Individual visual definition with Entity refs

TMDL Name Quoting Rules (Microsoft Spec):
  - Names with spaces, special chars, or reserved words MUST be quoted with single quotes
  - Examples:
    - table 'Customer Appointments'  (spaces - MUST quote)
    - table Sales                    (no spaces - no quote needed)
    - fromTable: 'My Table'          (spaces in relationship ref)

References:
  - Data Goblins: https://data-goblins.com/power-bi/programmatically-modify-reports
  - Microsoft PBIR: https://powerbi.microsoft.com/en-us/blog/power-bi-enhanced-report-format-pbir-in-power-bi-desktop-developer-mode-preview/
"""
import json
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# Characters that require quoting in TMDL names
TMDL_SPECIAL_CHARS = set(' \t\n\r\'\"[]{}().,;:!@#$%^&*+-=<>?/\\|`~')
TMDL_RESERVED_WORDS = {'table', 'column', 'measure', 'relationship', 'partition', 'hierarchy', 'level',
                       'annotation', 'expression', 'from', 'to', 'true', 'false', 'null'}


def needs_tmdl_quoting(name: str) -> bool:
    """
    Check if a TMDL name needs single quotes

    Rules:
    - Names with spaces need quotes
    - Names with special characters need quotes
    - Names starting with digits need quotes
    - Reserved words need quotes
    """
    if not name:
        return False

    # Check for spaces or special chars
    if any(c in TMDL_SPECIAL_CHARS for c in name):
        return True

    # Check if starts with digit
    if name[0].isdigit():
        return True

    # Check reserved words (case insensitive)
    if name.lower() in TMDL_RESERVED_WORDS:
        return True

    return False


def quote_tmdl_name(name: str) -> str:
    """
    Quote a TMDL name if needed

    Args:
        name: The name to potentially quote

    Returns:
        Quoted name if needed, otherwise original name
    """
    if needs_tmdl_quoting(name):
        # Escape any existing single quotes
        escaped = name.replace("'", "''")
        return f"'{escaped}'"
    return name


def unquote_tmdl_name(name: str) -> str:
    """Remove TMDL quotes from a name if present"""
    if name.startswith("'") and name.endswith("'"):
        # Remove outer quotes and unescape inner quotes
        return name[1:-1].replace("''", "'")
    return name


def quote_mcode_name(name: str) -> str:
    """
    Quote a name for M-Code/Power Query if needed.

    M-Code uses #"..." format for names with spaces or special characters.
    Names without spaces can be used directly.

    Examples:
        - Salesforce_Data -> Salesforce_Data (no quotes needed)
        - Leads Sales Data -> #"Leads Sales Data" (quotes required)
    """
    # Check if name needs quoting (has spaces or special chars)
    if ' ' in name or any(c in name for c in '!"#$%&\'()*+,-./:;<=>?@[\\]^`{|}~'):
        return f'#"{name}"'
    return name


def extract_external_refs(content: str) -> Tuple[str, Dict[str, str]]:
    """
    Extract ONLY external dataflow references from M-Code and replace with placeholders.

    External references like {[entity="TableName",version=""]} point to external
    data sources and should NEVER be modified during renames.

    Internal table references like Source = TableName SHOULD be modified.

    Returns:
        Tuple of (content_with_placeholders, {placeholder: original_ref})
    """
    external_refs = {}
    placeholder_counter = [0]

    def replace_external(match):
        placeholder = f"__EXTERNAL_REF_{placeholder_counter[0]}__"
        placeholder_counter[0] += 1
        external_refs[placeholder] = match.group(0)
        return placeholder

    # Pattern to match external dataflow references: {[entity="...",version="..."]}[Data]
    # This captures the entire external source reference pattern
    external_pattern = r'\{\[entity="[^"]+",version="[^"]*"\]\}\[Data\]'

    content_with_placeholders = re.sub(external_pattern, replace_external, content)

    return content_with_placeholders, external_refs


def restore_external_refs(content: str, external_refs: Dict[str, str]) -> str:
    """Restore external dataflow references from placeholders (unchanged)"""
    for placeholder, original in external_refs.items():
        content = content.replace(placeholder, original)
    return content


def fix_dax_table_references(dax_expression: str, table_names: List[str]) -> str:
    """
    Fix DAX expressions by quoting table names that have spaces or special chars.

    This handles cases like:
    - SUM(Leads Sales Data[Amount]) -> SUM('Leads Sales Data'[Amount])
    - CALCULATE(SUM(My Table[Col])) -> CALCULATE(SUM('My Table'[Col]))

    Args:
        dax_expression: The DAX expression to fix
        table_names: List of table names in the model (used for context)

    Returns:
        Fixed DAX expression with proper table name quoting
    """
    result = dax_expression

    for table_name in table_names:
        if needs_tmdl_quoting(table_name):
            # Find all unquoted references to this table followed by [
            # Matches: TableName[Column] but not 'TableName'[Column]
            pattern = rf"(?<!['\w]){re.escape(table_name)}(?=\s*\[)"
            replacement = quote_tmdl_name(table_name)
            result = re.sub(pattern, replacement, result)

            # Also fix function call patterns like: RELATED(TableName
            # but not: RELATED('TableName
            pattern2 = rf"(?<=[A-Za-z]\()\s*{re.escape(table_name)}(?=\s*[\[\,\)])"
            result = re.sub(pattern2, replacement, result)

    return result


@dataclass
class PBIPProject:
    """Represents a PBIP project structure"""
    root_path: Path
    pbip_file: Path
    semantic_model_folder: Optional[Path]
    report_folder: Optional[Path]
    report_json_path: Optional[Path]  # PBIR-Legacy: root report.json
    tmdl_files: List[Path]
    backup_path: Optional[Path] = None
    # PBIR Enhanced format fields
    is_pbir_enhanced: bool = False
    pbir_definition_folder: Optional[Path] = None  # Report/definition/ folder
    visual_json_files: List[Path] = field(default_factory=list)  # All visual.json files
    cultures_files: List[Path] = field(default_factory=list)  # Linguistic schema files
    # Additional semantic model files
    diagram_layout_path: Optional[Path] = None  # diagramLayout.json for model diagram


@dataclass
class ValidationError:
    """Represents a validation error found after rename"""
    file_path: str
    line_number: int
    error_type: str
    message: str
    context: str


@dataclass
class RenameResult:
    """Result of a PBIP rename operation"""
    success: bool
    message: str
    files_modified: List[str]
    references_updated: int
    details: Optional[Dict[str, Any]] = None
    validation_errors: List[ValidationError] = field(default_factory=list)
    backup_created: Optional[str] = None


class PowerBIPBIPConnector:
    """
    PBIP Connector for file-based Power BI Project editing

    Enables safe bulk renames by editing:
    - TMDL files (semantic model definitions)
    - report.json (visual field bindings)

    Key improvements in V2.1:
    - Proper TMDL quoting for names with spaces
    - Complete relationship reference updates (fromTable/toTable)
    - Automatic backup before changes
    - Post-change validation
    """

    def __init__(self, auto_backup: bool = True):
        self.current_project: Optional[PBIPProject] = None
        self.auto_backup = auto_backup
        self._original_files: Dict[str, str] = {}  # For rollback

    @staticmethod
    def find_pbip_project_from_model_name(model_name: str, search_paths: Optional[List[str]] = None) -> Optional[PBIPProject]:
        """
        Find a PBIP project that matches the given model name

        Args:
            model_name: Name of the model (usually the database name from TOM)
            search_paths: Optional list of paths to search for PBIP projects

        Returns:
            PBIPProject if found, None otherwise
        """
        if not search_paths:
            # Default search locations
            search_paths = [
                os.path.expanduser("~/Documents"),
                os.path.expanduser("~/Desktop"),
                os.path.expanduser("~/Downloads"),
                "C:/",
            ]

        # Also check common development folders
        for base in ["C:/Users", os.path.expanduser("~")]:
            for folder in ["Projects", "Work", "Dev", "GitHub", "Repos"]:
                path = os.path.join(base, folder)
                if os.path.exists(path):
                    search_paths.append(path)

        for search_path in search_paths:
            if not os.path.exists(search_path):
                continue

            # Look for .pbip files
            try:
                for root, dirs, files in os.walk(search_path):
                    # Limit depth to avoid searching too deep
                    depth = root.replace(search_path, '').count(os.sep)
                    if depth > 5:
                        continue

                    for file in files:
                        if file.endswith('.pbip'):
                            pbip_path = Path(root) / file
                            project = PowerBIPBIPConnector._parse_pbip_project(pbip_path)
                            if project:
                                # Check if model name matches
                                if project.semantic_model_folder and model_name.lower() in project.semantic_model_folder.name.lower():
                                    logger.info(f"Found PBIP project for model '{model_name}' at: {pbip_path}")
                                    return project
            except PermissionError:
                continue

        return None

    @staticmethod
    def find_pbip_from_path(pbip_path: str) -> Optional[PBIPProject]:
        """
        Parse a PBIP project from a given path

        Args:
            pbip_path: Path to the .pbip file or project folder

        Returns:
            PBIPProject if valid, None otherwise
        """
        path = Path(pbip_path)

        # If it's a folder, look for .pbip file inside
        if path.is_dir():
            pbip_files = list(path.glob("*.pbip"))
            if pbip_files:
                path = pbip_files[0]
            else:
                return None

        if not path.exists() or not path.suffix == '.pbip':
            return None

        return PowerBIPBIPConnector._parse_pbip_project(path)

    @staticmethod
    def _parse_pbip_project(pbip_path: Path) -> Optional[PBIPProject]:
        """Parse a PBIP project structure, detecting both PBIR-Legacy and PBIR Enhanced formats"""
        try:
            root = pbip_path.parent

            # Find semantic model folder (.SemanticModel)
            semantic_folders = list(root.glob("*.SemanticModel"))
            semantic_model_folder = semantic_folders[0] if semantic_folders else None

            # Find report folder (.Report)
            report_folders = list(root.glob("*.Report"))
            report_folder = report_folders[0] if report_folders else None

            # Initialize PBIR fields
            report_json_path = None
            is_pbir_enhanced = False
            pbir_definition_folder = None
            visual_json_files = []
            cultures_files = []

            if report_folder:
                # Check for PBIR Enhanced format: Report/definition/pages/ structure
                definition_folder = report_folder / "definition"
                pages_folder = definition_folder / "pages"

                if pages_folder.exists():
                    # This is PBIR Enhanced format
                    is_pbir_enhanced = True
                    pbir_definition_folder = definition_folder

                    # Find all visual.json files in pages/[id]/visuals/[id]/
                    visual_json_files = list(pages_folder.glob("**/visuals/*/visual.json"))

                    # Report.json in definition folder (report-level settings only)
                    report_json = definition_folder / "report.json"
                    if report_json.exists():
                        report_json_path = report_json

                    logger.info(f"Detected PBIR Enhanced format with {len(visual_json_files)} visual files")
                else:
                    # Check for PBIR-Legacy: single report.json at root
                    report_json = report_folder / "report.json"
                    if report_json.exists():
                        report_json_path = report_json
                        logger.info("Detected PBIR-Legacy format (single report.json)")

            # Find all TMDL files
            tmdl_files = []
            diagram_layout_path = None
            if semantic_model_folder:
                tmdl_files = list(semantic_model_folder.glob("**/*.tmdl"))
                tmdl_files.extend(semantic_model_folder.glob("**/*.tmd"))

                # Find cultures files (linguistic schema with ConceptualEntity)
                cultures_folder = semantic_model_folder / "definition" / "cultures"
                if cultures_folder.exists():
                    cultures_files = list(cultures_folder.glob("*.tmdl"))

                # Find diagramLayout.json (model diagram layout)
                diagram_layout = semantic_model_folder / "diagramLayout.json"
                if diagram_layout.exists():
                    diagram_layout_path = diagram_layout
                    logger.info("Found diagramLayout.json")

            return PBIPProject(
                root_path=root,
                pbip_file=pbip_path,
                semantic_model_folder=semantic_model_folder,
                report_folder=report_folder,
                report_json_path=report_json_path,
                tmdl_files=tmdl_files,
                is_pbir_enhanced=is_pbir_enhanced,
                pbir_definition_folder=pbir_definition_folder,
                visual_json_files=visual_json_files,
                cultures_files=cultures_files,
                diagram_layout_path=diagram_layout_path
            )

        except Exception as e:
            logger.error(f"Failed to parse PBIP project: {e}")
            return None

    def load_project(self, pbip_path: str) -> bool:
        """
        Load a PBIP project for editing

        Args:
            pbip_path: Path to .pbip file or project folder

        Returns:
            True if loaded successfully
        """
        project = self.find_pbip_from_path(pbip_path)
        if project:
            self.current_project = project
            self._original_files = {}  # Clear any previous backup state
            logger.info(f"Loaded PBIP project: {project.pbip_file}")
            return True
        return False

    def get_project_info(self) -> Dict[str, Any]:
        """Get information about the loaded project"""
        if not self.current_project:
            return {"error": "No project loaded"}

        return {
            "root_path": str(self.current_project.root_path),
            "pbip_file": str(self.current_project.pbip_file),
            "semantic_model_folder": str(self.current_project.semantic_model_folder) if self.current_project.semantic_model_folder else None,
            "report_folder": str(self.current_project.report_folder) if self.current_project.report_folder else None,
            "report_json_path": str(self.current_project.report_json_path) if self.current_project.report_json_path else None,
            "tmdl_file_count": len(self.current_project.tmdl_files),
            "has_report": self.current_project.report_json_path is not None,
            # PBIR Enhanced format info
            "report_format": "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy",
            "is_pbir_enhanced": self.current_project.is_pbir_enhanced,
            "visual_json_count": len(self.current_project.visual_json_files),
            "cultures_file_count": len(self.current_project.cultures_files),
            "pbir_definition_folder": str(self.current_project.pbir_definition_folder) if self.current_project.pbir_definition_folder else None
        }

    # ==================== LIST VISUALS ====================

    def list_visuals(self) -> Dict[str, Any]:
        """
        List all visuals in the loaded PBIP project.

        Supports both report formats:
        - PBIR-Legacy: Single report.json with all visuals
        - PBIR-Enhanced: Individual visual.json files organized by pages

        Returns:
            Dict with visual information by page/section
        """
        if not self.current_project:
            return {"error": "No project loaded", "visuals": []}

        visuals = []

        try:
            if self.current_project.is_pbir_enhanced:
                # PBIR-Enhanced: Extract from visual.json files
                visuals = self._list_visuals_from_pbir_enhanced()
            elif self.current_project.report_json_path:
                # PBIR-Legacy: Extract from single report.json
                visuals = self._list_visuals_from_pbir_legacy()
        except Exception as e:
            logger.error(f"Error listing visuals: {e}")
            return {"error": str(e), "visuals": []}

        return {
            "success": True,
            "format": "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy",
            "total_visuals": len(visuals),
            "visuals": visuals
        }

    def _list_visuals_from_pbir_enhanced(self) -> List[Dict[str, Any]]:
        """Extract visuals from PBIR-Enhanced format (individual visual.json files)"""
        visuals = []

        if not self.current_project or not self.current_project.pbir_definition_folder:
            return visuals

        try:
            pages_folder = self.current_project.pbir_definition_folder / "pages"
            if not pages_folder.exists():
                return visuals

            # List all pages
            for page_folder in sorted(pages_folder.iterdir()):
                if not page_folder.is_dir():
                    continue

                page_id = page_folder.name
                page_json = page_folder / "page.json"
                page_name = page_id  # Default to ID

                # Get page name from page.json if available
                if page_json.exists():
                    try:
                        with open(page_json, 'r', encoding='utf-8') as f:
                            page_data = json.load(f)
                            page_name = page_data.get("name", page_name)
                            page_name = page_data.get("displayName", page_name)
                    except Exception:
                        pass

                # List all visuals in this page
                visuals_folder = page_folder / "visuals"
                if visuals_folder.exists():
                    for visual_folder in sorted(visuals_folder.iterdir()):
                        if not visual_folder.is_dir():
                            continue

                        visual_id = visual_folder.name
                        visual_json = visual_folder / "visual.json"

                        if visual_json.exists():
                            visual_info = {
                                "page_id": page_id,
                                "page_name": page_name,
                                "visual_id": visual_id,
                                "visual_name": visual_id,  # Default to ID
                                "type": "Unknown",
                                "config": {}
                            }

                            # Parse visual.json for more details
                            try:
                                with open(visual_json, 'r', encoding='utf-8') as f:
                                    visual_data = json.load(f)

                                    # Extract visual name
                                    if "config" in visual_data:
                                        config = visual_data["config"]
                                        visual_info["visual_name"] = config.get("name", visual_id)
                                        visual_info["type"] = config.get("visualType", "Unknown")

                                        # Extract key properties
                                        if "layouts" in config:
                                            layouts = config["layouts"]
                                            if layouts and len(layouts) > 0:
                                                visual_info["dimensions"] = {
                                                    "width": layouts[0].get("width", 0),
                                                    "height": layouts[0].get("height", 0),
                                                    "x": layouts[0].get("x", 0),
                                                    "y": layouts[0].get("y", 0)
                                                }

                                        # Extract data bindings
                                        if "objects" in config:
                                            visual_info["has_bindings"] = True
                                            visual_info["binding_count"] = len(config["objects"])

                                    # Extract source Entity (table reference)
                                    if "config" in visual_data and "prototypeQuery" in visual_data["config"]:
                                        prototype = visual_data["config"]["prototypeQuery"]
                                        if "Where" in prototype:  # DAX queries
                                            visual_info["uses_filters"] = True
                                        if "From" in prototype:
                                            visual_info["from_source"] = prototype["From"]

                            except (json.JSONDecodeError, KeyError) as e:
                                logger.warning(f"Could not fully parse visual {visual_id}: {e}")

                            visuals.append(visual_info)

        except Exception as e:
            logger.error(f"Error listing PBIR-Enhanced visuals: {e}")

        return visuals

    def _list_visuals_from_pbir_legacy(self) -> List[Dict[str, Any]]:
        """Extract visuals from PBIR-Legacy format (single report.json)"""
        visuals = []

        if not self.current_project or not self.current_project.report_json_path:
            return visuals

        try:
            with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                report_data = json.load(f)

            # Extract sections (pages) from report
            sections = report_data.get("sections", [])

            for section in sections:
                section_id = section.get("name", "Unknown")
                section_name = section.get("displayName", section_id)

                # List visuals in this section
                visual_containers = section.get("visualContainers", [])

                for visual_container in visual_containers:
                    visual_config = visual_container.get("config", {})
                    visual_name = visual_config.get("name", "Unnamed Visual")
                    visual_type = visual_config.get("visualType", "Unknown")

                    visual_info = {
                        "page_id": section_id,
                        "page_name": section_name,
                        "visual_name": visual_name,
                        "visual_id": visual_container.get("name", "unknown_id"),
                        "type": visual_type,
                        "config": visual_config.get("query", {})
                    }

                    # Extract dimensions
                    if "layouts" in visual_config:
                        layouts = visual_config["layouts"]
                        if layouts and len(layouts) > 0:
                            visual_info["dimensions"] = {
                                "width": layouts[0].get("width", 0),
                                "height": layouts[0].get("height", 0),
                                "x": layouts[0].get("x", 0),
                                "y": layouts[0].get("y", 0)
                            }

                    # Extract binding/entity info
                    if "objects" in visual_config and visual_config["objects"]:
                        objects = visual_config["objects"]
                        visual_info["has_bindings"] = True
                        visual_info["binding_count"] = len(objects)

                        # Try to find Entity references
                        if "dataRoles" in objects:
                            virtual_roles = objects["dataRoles"]
                            for role in virtual_roles:
                                if "queryName" in role:
                                    visual_info["query_name"] = role["queryName"]

                    visuals.append(visual_info)

        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error listing PBIR-Legacy visuals: {e}")

        return visuals

    # ==================== BACKUP & ROLLBACK ====================

    def create_backup(self) -> Optional[str]:
        """
        Create a backup of the entire PBIP project

        Returns:
            Path to backup folder, or None if failed
        """
        if not self.current_project:
            return None

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{self.current_project.pbip_file.stem}_backup_{timestamp}"
            backup_path = self.current_project.root_path.parent / backup_name

            # Copy entire project folder
            shutil.copytree(self.current_project.root_path, backup_path)

            self.current_project.backup_path = backup_path
            logger.info(f"Created backup at: {backup_path}")
            return str(backup_path)

        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return None

    def _cache_file_content(self, file_path: Path) -> None:
        """Cache original file content for potential rollback"""
        if str(file_path) not in self._original_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    self._original_files[str(file_path)] = f.read()
            except Exception as e:
                logger.warning(f"Could not cache file {file_path}: {e}")

    def rollback_changes(self) -> bool:
        """
        Rollback all changes made in this session

        Returns:
            True if rollback successful
        """
        if not self._original_files:
            logger.warning("No changes to rollback")
            return False

        try:
            for file_path, content in self._original_files.items():
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)

            logger.info(f"Rolled back {len(self._original_files)} file(s)")
            self._original_files = {}
            return True

        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return False

    # ==================== VALIDATION ====================

    def validate_tmdl_syntax(self) -> List[ValidationError]:
        """
        Validate TMDL files for common syntax errors

        Returns:
            List of validation errors found
        """
        errors = []

        if not self.current_project or not self.current_project.tmdl_files:
            return errors

        # Build set of table names that need quoting
        tables_needing_quotes = set()
        try:
            for tmdl_file in self.current_project.tmdl_files:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Find all table declarations (handle both quoted and unquoted names)
                # Pattern 1: table 'Name With Spaces'
                for match in re.finditer(r"^table\s+'([^']+)'", content, re.MULTILINE):
                    table_name = match.group(1).replace("''", "'")  # Unescape quotes
                    if needs_tmdl_quoting(table_name):
                        tables_needing_quotes.add(table_name)
                # Pattern 2: table UnquotedName
                for match in re.finditer(r"^table\s+(\w+)\s*$", content, re.MULTILINE):
                    table_name = match.group(1)
                    if needs_tmdl_quoting(table_name):
                        tables_needing_quotes.add(table_name)
        except Exception:
            pass

        for tmdl_file in self.current_project.tmdl_files:
            try:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

                for i, line in enumerate(lines, 1):
                    stripped = line.strip()

                    # Check for unquoted names with spaces in declarations
                    # Pattern: "table Name With Spaces" (without quotes)
                    if stripped.startswith('table '):
                        name_part = stripped[6:].strip()
                        if ' ' in name_part and not name_part.startswith("'"):
                            errors.append(ValidationError(
                                file_path=str(tmdl_file),
                                line_number=i,
                                error_type="UNQUOTED_NAME",
                                message=f"Table name with spaces must be quoted: {name_part}",
                                context=stripped
                            ))

                    # Check fromTable/toTable references
                    for prefix in ['fromTable:', 'toTable:']:
                        if prefix in stripped:
                            # Extract the name after the prefix
                            match = re.search(rf'{prefix}\s*(.+?)(?:\s*$|\s+\w+:)', stripped)
                            if match:
                                name_part = match.group(1).strip()
                                if ' ' in name_part and not name_part.startswith("'"):
                                    errors.append(ValidationError(
                                        file_path=str(tmdl_file),
                                        line_number=i,
                                        error_type="UNQUOTED_REFERENCE",
                                        message=f"Relationship reference with spaces must be quoted: {name_part}",
                                        context=stripped
                                    ))

                    # Check for unquoted table references in DAX (measure/column expressions)
                    if 'expression:' in stripped or '=' in stripped:
                        # Check for unquoted table references that need quoting
                        for table_name in tables_needing_quotes:
                            # Pattern: unquoted TableName[Column] where table has spaces
                            if re.search(rf"(?<!['\w]){re.escape(table_name)}(?=\s*\[)", stripped):
                                errors.append(ValidationError(
                                    file_path=str(tmdl_file),
                                    line_number=i,
                                    error_type="UNQUOTED_TABLE_IN_DAX",
                                    message=f"Table '{table_name}' in DAX expression must be quoted: use '{quote_tmdl_name(table_name)}' instead of '{table_name}'",
                                    context=stripped
                                ))
                                break  # Only report once per line

            except Exception as e:
                errors.append(ValidationError(
                    file_path=str(tmdl_file),
                    line_number=0,
                    error_type="FILE_ERROR",
                    message=str(e),
                    context=""
                ))

        return errors

    def fix_all_dax_quoting(self) -> Dict[str, Any]:
        """
        Fix all DAX expressions in TMDL files by properly quoting table names with spaces.

        This scans all measures and expressions and quotes table names that have spaces
        but are referenced without quotes.

        Returns:
            Dict with files_modified, references_fixed count, and validation_errors
        """
        if not self.current_project or not self.current_project.tmdl_files:
            return {"files_modified": [], "count": 0, "errors": []}

        files_modified = []
        total_fixes = 0
        errors = []

        # First, collect all table names in the project
        table_names = set()
        try:
            for tmdl_file in self.current_project.tmdl_files:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Find all table declarations - both quoted and unquoted
                # Pattern 1: table 'Name With Spaces'
                for match in re.finditer(r"^(?:\s*)table\s+'([^']+)'", content, re.MULTILINE):
                    table_name = match.group(1).replace("''", "'")  # Unescape quotes
                    table_names.add(table_name)
                # Pattern 2: table UnquotedName
                for match in re.finditer(r"^(?:\s*)table\s+(\w+)\s*$", content, re.MULTILINE):
                    table_names.add(match.group(1))
        except Exception as e:
            logger.warning(f"Could not extract table names: {e}")

        # Filter to only tables that need quoting
        tables_needing_quotes = [t for t in table_names if needs_tmdl_quoting(t)]

        if not tables_needing_quotes:
            return {"files_modified": [], "count": 0, "errors": []}

        # Now process each file and fix DAX expressions
        for tmdl_file in self.current_project.tmdl_files:
            try:
                self._cache_file_content(tmdl_file)

                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_fixes = 0

                # Fix each table that needs quoting
                for table_name in tables_needing_quotes:
                    table_quoted = quote_tmdl_name(table_name)
                    escaped_name = re.escape(table_name)

                    # Pattern 1: TableName[Column] -> 'TableName'[Column]
                    pattern1 = rf"(?<!['\w]){escaped_name}(?=\s*\[)"
                    content_before = content
                    content = re.sub(pattern1, table_quoted, content)
                    file_fixes += len(re.findall(pattern1, content_before))

                    # Pattern 2: Handle function calls like CALCULATE(SUM(TableName[Col]))
                    # This pattern matches unquoted table names in DAX contexts
                    pattern2 = rf"(\()\s*{escaped_name}(?=\s*[\[\,\)])"
                    content_before = content
                    content = re.sub(pattern2, rf"\1{table_quoted}", content)
                    file_fixes += len(re.findall(pattern2, content_before))

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_fixes += file_fixes
                    logger.info(f"Fixed {file_fixes} DAX quote references in {tmdl_file}")

            except Exception as e:
                logger.error(f"Error fixing DAX in {tmdl_file}: {e}")
                errors.append({"file": str(tmdl_file), "error": str(e)})

        return {
            "files_modified": files_modified,
            "count": total_fixes,
            "tables_fixed": tables_needing_quotes,
            "errors": errors
        }

    # ==================== RENAME OPERATIONS ====================

    def rename_table_in_files(self, old_name: str, new_name: str) -> RenameResult:
        """
        Rename a table across all PBIP files (TMDL + report visuals)

        This is a COMPREHENSIVE rename that:
        1. Updates table declarations in TMDL files
        2. Updates ALL DAX references with proper quoting (if new name has spaces)
        3. Updates report layer (PBIR-Legacy or PBIR-Enhanced visual.json files)
        4. Updates cultures files (linguistic schema)

        Supports both PBIR-Legacy (report.json) and PBIR-Enhanced (visual.json files) formats.

        Args:
            old_name: Current table name
            new_name: New table name

        Returns:
            RenameResult with details
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        files_modified = []
        total_replacements = 0
        backup_path = None

        # Create backup if enabled
        if self.auto_backup and not self.current_project.backup_path:
            backup_path = self.create_backup()

        # 1. Update TMDL files (semantic model) - auto-quoting is built-in
        tmdl_replacements = self._rename_table_in_tmdl_files(old_name, new_name)
        files_modified.extend(tmdl_replacements["files"])
        total_replacements += tmdl_replacements["count"]

        # 2. Update report layer (PBIR-Legacy or PBIR-Enhanced)
        if self.current_project.is_pbir_enhanced:
            # PBIR Enhanced: Update individual visual.json files
            visual_replacements = self._rename_table_in_visual_files(old_name, new_name)
            files_modified.extend(visual_replacements["files"])
            total_replacements += visual_replacements["count"]
        elif self.current_project.report_json_path:
            # PBIR Legacy: Update single report.json
            report_replacements = self._rename_table_in_report_json(old_name, new_name)
            if report_replacements["count"] > 0:
                files_modified.append(str(self.current_project.report_json_path))
                total_replacements += report_replacements["count"]

        # 3. Update cultures files (linguistic schema)
        if self.current_project.cultures_files:
            cultures_replacements = self._rename_table_in_cultures_files(old_name, new_name)
            files_modified.extend(cultures_replacements["files"])
            total_replacements += cultures_replacements["count"]

        # 4. Update diagramLayout.json (model diagram)
        if self.current_project.diagram_layout_path:
            diagram_replacements = self._rename_table_in_diagram_layout(old_name, new_name)
            files_modified.extend(diagram_replacements["files"])
            total_replacements += diagram_replacements["count"]

        # 5. Validate after changes
        validation_errors = self.validate_tmdl_syntax()

        report_format = "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy"

        return RenameResult(
            success=len(validation_errors) == 0,
            message=f"Renamed table '{old_name}' to '{new_name}' in {len(files_modified)} file(s) ({report_format})" +
                    (f" with {len(validation_errors)} validation error(s)" if validation_errors else ""),
            files_modified=files_modified,
            references_updated=total_replacements,
            details={"old_name": old_name, "new_name": new_name, "report_format": report_format},
            validation_errors=validation_errors,
            backup_created=backup_path
        )

    def rename_column_in_files(self, table_name: str, old_name: str, new_name: str) -> RenameResult:
        """
        Rename a column across all PBIP files

        Supports both PBIR-Legacy and PBIR-Enhanced formats.

        Args:
            table_name: Table containing the column
            old_name: Current column name
            new_name: New column name

        Returns:
            RenameResult with details
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        files_modified = []
        total_replacements = 0
        backup_path = None

        # Create backup if enabled
        if self.auto_backup and not self.current_project.backup_path:
            backup_path = self.create_backup()

        # 1. Update TMDL files (semantic model)
        tmdl_replacements = self._rename_column_in_tmdl_files(table_name, old_name, new_name)
        files_modified.extend(tmdl_replacements["files"])
        total_replacements += tmdl_replacements["count"]

        # 2. Update report layer (PBIR-Legacy or PBIR-Enhanced)
        if self.current_project.is_pbir_enhanced:
            # PBIR Enhanced: Update individual visual.json files
            visual_replacements = self._rename_column_in_visual_files(table_name, old_name, new_name)
            files_modified.extend(visual_replacements["files"])
            total_replacements += visual_replacements["count"]
        elif self.current_project.report_json_path:
            # PBIR Legacy: Update single report.json
            report_replacements = self._rename_column_in_report_json(table_name, old_name, new_name)
            if report_replacements["count"] > 0:
                files_modified.append(str(self.current_project.report_json_path))
                total_replacements += report_replacements["count"]

        report_format = "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy"

        return RenameResult(
            success=True,
            message=f"Renamed column '{table_name}'[{old_name}] to [{new_name}] in {len(files_modified)} file(s) ({report_format})",
            files_modified=files_modified,
            references_updated=total_replacements,
            details={"table_name": table_name, "old_name": old_name, "new_name": new_name, "report_format": report_format},
            backup_created=backup_path
        )

    def rename_measure_in_files(self, old_name: str, new_name: str) -> RenameResult:
        """
        Rename a measure across all PBIP files

        Supports both PBIR-Legacy and PBIR-Enhanced formats.

        Args:
            old_name: Current measure name
            new_name: New measure name

        Returns:
            RenameResult with details
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        files_modified = []
        total_replacements = 0
        backup_path = None

        # Create backup if enabled
        if self.auto_backup and not self.current_project.backup_path:
            backup_path = self.create_backup()

        # 1. Update TMDL files (semantic model)
        tmdl_replacements = self._rename_measure_in_tmdl_files(old_name, new_name)
        files_modified.extend(tmdl_replacements["files"])
        total_replacements += tmdl_replacements["count"]

        # 2. Update report layer (PBIR-Legacy or PBIR-Enhanced)
        if self.current_project.is_pbir_enhanced:
            # PBIR Enhanced: Update individual visual.json files
            visual_replacements = self._rename_measure_in_visual_files(old_name, new_name)
            files_modified.extend(visual_replacements["files"])
            total_replacements += visual_replacements["count"]
        elif self.current_project.report_json_path:
            # PBIR Legacy: Update single report.json
            report_replacements = self._rename_measure_in_report_json(old_name, new_name)
            if report_replacements["count"] > 0:
                files_modified.append(str(self.current_project.report_json_path))
                total_replacements += report_replacements["count"]

        report_format = "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy"

        return RenameResult(
            success=True,
            message=f"Renamed measure '{old_name}' to '{new_name}' in {len(files_modified)} file(s) ({report_format})",
            files_modified=files_modified,
            references_updated=total_replacements,
            details={"old_name": old_name, "new_name": new_name, "report_format": report_format},
            backup_created=backup_path
        )

    # ==================== TMDL FILE OPERATIONS ====================

    def _rename_table_in_tmdl_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in TMDL files with proper quoting

        Handles:
        1. Table declarations: table OldName -> table 'New Name'
        2. DAX references: 'OldName'[Column] -> 'New Name'[Column]
        3. Relationship refs: fromTable: OldName -> fromTable: 'New Name'
        4. Relationship names: 'OldName to Table' -> 'New Name to Table'
        """
        files_modified = []
        total_count = 0

        if not self.current_project or not self.current_project.tmdl_files:
            return {"files": files_modified, "count": total_count}

        # Properly quoted new name for TMDL
        new_name_quoted = quote_tmdl_name(new_name)

        # Build regex patterns for old name (handle both quoted and unquoted)
        old_name_escaped = re.escape(old_name)
        old_name_quoted_escaped = re.escape(f"'{old_name}'")

        # Build patterns list
        patterns = []

        # Pattern 1: Table declaration (both quoted and unquoted old names)
        # table OldName -> table 'New Name'
        patterns.append((
            rf'^(\s*)table\s+{old_name_escaped}\s*$',
            rf'\1table {new_name_quoted}',
            re.MULTILINE
        ))
        # table 'OldName' -> table 'New Name'
        patterns.append((
            rf"^(\s*)table\s+'{old_name_escaped}'\s*$",
            rf'\1table {new_name_quoted}',
            re.MULTILINE
        ))

        # Pattern 2: DAX references - 'TableName'[Column]
        patterns.append((
            rf"'{old_name_escaped}'\s*\[",
            f"{new_name_quoted}[",
            0
        ))

        # Pattern 3: DAX references - unquoted TableName[Column]
        # IMPORTANT: Always use the properly quoted version for the new name
        patterns.append((
            rf"(?<!['\w]){old_name_escaped}(?=\s*\[)",
            new_name_quoted,
            0
        ))

        # Pattern 4: 'TableName' in function calls (RELATED, ALL, VALUES, etc.)
        patterns.append((
            rf"'{old_name_escaped}'(?=\s*[,\)\]])",
            new_name_quoted,
            0
        ))

        # Pattern 4b: Unquoted TableName in function calls
        # This catches: FUNCTION(OldTable[, FUNCTION(OldTable,, FUNCTION(OldTable)
        # Examples: COUNTROWS(TableName), SUM(TableName[Col]), FILTER(TableName, ...)
        patterns.append((
            rf"([A-Z]+\s*\(\s*){old_name_escaped}(?=\s*[\[\,\)])",
            rf"\1{new_name_quoted}",
            re.IGNORECASE
        ))

        # Pattern 4c: Relationship names containing table name (CRITICAL for relationships)
        # Handle: relationship 'OldName to SomeTable' -> relationship 'NewName to SomeTable'
        # The entire name is in quotes, so we need to replace just the table name part inside quotes
        patterns.append((
            rf"(relationship\s+')({old_name_escaped})(\s+to\s+[^']*)'",
            rf"\1{new_name}\3'",
            re.IGNORECASE
        ))
        # Handle: relationship 'SomeTable to OldName' -> relationship 'SomeTable to NewName'
        patterns.append((
            rf"(relationship\s+')([^']*\s+to\s+)({old_name_escaped})'",
            rf"\1\2{new_name}'",
            re.IGNORECASE
        ))
        # Handle unquoted: relationship OldName to SomeTable -> relationship 'NewName' to SomeTable
        patterns.append((
            rf"(relationship\s+){old_name_escaped}(\s+to\s+)",
            rf"\1{new_name_quoted}\2",
            re.IGNORECASE
        ))
        # Handle unquoted: relationship SomeTable to OldName -> relationship SomeTable to 'NewName'
        patterns.append((
            rf"(relationship\s+)(\S+\s+to\s+){old_name_escaped}(?=\s|$)",
            rf"\1\2{new_name_quoted}",
            re.IGNORECASE | re.MULTILINE
        ))

        # Pattern 5: fromTable reference (CRITICAL for relationships)
        # fromTable: OldName -> fromTable: 'New Name'
        patterns.append((
            rf'(fromTable\s*:\s*){old_name_escaped}(?=\s*(?:$|\n|\r|toTable|fromColumn|toColumn))',
            rf'\1{new_name_quoted}',
            re.MULTILINE | re.IGNORECASE
        ))
        # fromTable: 'OldName' -> fromTable: 'New Name'
        patterns.append((
            rf"(fromTable\s*:\s*)'{old_name_escaped}'",
            rf'\1{new_name_quoted}',
            re.IGNORECASE
        ))

        # Pattern 6: toTable reference (CRITICAL for relationships)
        # toTable: OldName -> toTable: 'New Name'
        patterns.append((
            rf'(toTable\s*:\s*){old_name_escaped}(?=\s*(?:$|\n|\r|fromTable|fromColumn|toColumn))',
            rf'\1{new_name_quoted}',
            re.MULTILINE | re.IGNORECASE
        ))
        # toTable: 'OldName' -> toTable: 'New Name'
        patterns.append((
            rf"(toTable\s*:\s*)'{old_name_escaped}'",
            rf'\1{new_name_quoted}',
            re.IGNORECASE
        ))

        # Pattern 7: Partition name (partition OldName = m)
        # partition OldName -> partition 'New Name'
        patterns.append((
            rf'^(\s*)partition\s+{old_name_escaped}\s*=',
            rf'\1partition {new_name_quoted} =',
            re.MULTILINE
        ))
        # partition 'OldName' -> partition 'New Name'
        patterns.append((
            rf"^(\s*)partition\s+'{old_name_escaped}'\s*=",
            rf'\1partition {new_name_quoted} =',
            re.MULTILINE
        ))

        # Pattern 8: fromColumn with table prefix (CRITICAL for relationships)
        # Format: fromColumn: TableName.ColumnName or fromColumn: TableName.'Column Name'
        # fromColumn: OldTable.Column -> fromColumn: 'NewTable'.Column
        patterns.append((
            rf'(fromColumn\s*:\s*){old_name_escaped}\.(\w+)',
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # fromColumn: OldTable.'Column Name' -> fromColumn: 'NewTable'.'Column Name'
        patterns.append((
            rf"(fromColumn\s*:\s*){old_name_escaped}\.('[^']+')",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # fromColumn: 'OldTable'.Column -> fromColumn: 'NewTable'.Column
        patterns.append((
            rf"(fromColumn\s*:\s*)'{old_name_escaped}'\.(\w+)",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # fromColumn: 'OldTable'.'Column Name' -> fromColumn: 'NewTable'.'Column Name'
        patterns.append((
            rf"(fromColumn\s*:\s*)'{old_name_escaped}'\.('[^']+')",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))

        # Pattern 9: toColumn with table prefix (CRITICAL for relationships)
        # Same patterns as fromColumn but for toColumn
        # toColumn: OldTable.Column -> toColumn: 'NewTable'.Column
        patterns.append((
            rf'(toColumn\s*:\s*){old_name_escaped}\.(\w+)',
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # toColumn: OldTable.'Column Name' -> toColumn: 'NewTable'.'Column Name'
        patterns.append((
            rf"(toColumn\s*:\s*){old_name_escaped}\.('[^']+')",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # toColumn: 'OldTable'.Column -> toColumn: 'NewTable'.Column
        patterns.append((
            rf"(toColumn\s*:\s*)'{old_name_escaped}'\.(\w+)",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))
        # toColumn: 'OldTable'.'Column Name' -> toColumn: 'NewTable'.'Column Name'
        patterns.append((
            rf"(toColumn\s*:\s*)'{old_name_escaped}'\.('[^']+')",
            rf'\1{new_name_quoted}.\2',
            re.MULTILINE
        ))

        # Pattern 10: ref table in model.tmdl
        # ref table OldName -> ref table 'NewName'
        patterns.append((
            rf'^(\s*)ref\s+table\s+{old_name_escaped}\s*$',
            rf'\1ref table {new_name_quoted}',
            re.MULTILINE
        ))
        # ref table 'OldName' -> ref table 'NewName'
        patterns.append((
            rf"^(\s*)ref\s+table\s+'{old_name_escaped}'\s*$",
            rf'\1ref table {new_name_quoted}',
            re.MULTILINE
        ))

        # Pattern 11: PBI_QueryOrder annotation (list of table names in JSON array format)
        # IMPORTANT: Must be specific to PBI_QueryOrder to avoid matching M-Code entity= references
        # Format: annotation PBI_QueryOrder = ["Table1","Table2",...]
        # DO NOT match: {[entity="TableName",version=""]} (M-Code/Power Query source references)
        patterns.append((
            rf'(PBI_QueryOrder\s*=\s*\[.*?")({old_name_escaped})(".*?\])',
            rf'\1{new_name}\3',
            0
        ))

        # Pattern 12: M-Code internal table references
        # These are references to other tables in the same model within Power Query
        # Format: VariableName = TableName, (inside let ... in blocks)
        # Uses #"..." quoting for names with spaces
        new_name_mcode = quote_mcode_name(new_name)

        # = OldName, (assignment with comma - most common in let blocks)
        patterns.append((
            rf'(=\s*){old_name_escaped}(\s*,)',
            rf'\1{new_name_mcode}\2',
            0
        ))
        # = #"OldName", (quoted assignment with comma)
        patterns.append((
            rf'(=\s*)#"{old_name_escaped}"(\s*,)',
            rf'\1{new_name_mcode}\2',
            0
        ))
        # = OldName at end of line (no comma - last line in let block or simple query)
        patterns.append((
            rf'(=\s*){old_name_escaped}(\s*)$',
            rf'\1{new_name_mcode}\2',
            re.MULTILINE
        ))
        # = #"OldName" at end of line
        patterns.append((
            rf'(=\s*)#"{old_name_escaped}"(\s*)$',
            rf'\1{new_name_mcode}\2',
            re.MULTILINE
        ))

        for tmdl_file in self.current_project.tmdl_files:
            try:
                # Cache original content for rollback
                self._cache_file_content(tmdl_file)

                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # CRITICAL: Extract ONLY external dataflow references
                # External refs like {[entity="...",version=""]}[Data] must NEVER change
                # Internal table refs like Source = TableName SHOULD change
                content, external_refs = extract_external_refs(content)

                for pattern, replacement, flags in patterns:
                    content, count = re.subn(pattern, replacement, content, flags=flags)
                    file_count += count

                # Restore external references (unchanged)
                content = restore_external_refs(content, external_refs)

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_count += file_count
                    logger.info(f"Updated {file_count} references in {tmdl_file}")

            except Exception as e:
                logger.error(f"Error updating TMDL file {tmdl_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_column_in_tmdl_files(self, table_name: str, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename column references in TMDL files with proper quoting"""
        files_modified = []
        total_count = 0

        if not self.current_project or not self.current_project.tmdl_files:
            return {"files": files_modified, "count": total_count}

        # Quote names if needed
        table_name_quoted = quote_tmdl_name(table_name)
        new_name_quoted = quote_tmdl_name(new_name)

        # For DAX, columns inside brackets don't need external quotes but may need them if they have spaces
        new_col_in_bracket = new_name if not needs_tmdl_quoting(new_name) else new_name

        # Escape for regex
        table_escaped = re.escape(table_name)
        old_escaped = re.escape(old_name)

        # Patterns for column references
        patterns = [
            # 'TableName'[OldColumn] -> 'TableName'[NewColumn]
            (rf"'{table_escaped}'\s*\[\s*{old_escaped}\s*\]", f"{table_name_quoted}[{new_col_in_bracket}]", 0),
            # TableName[OldColumn] -> TableName[NewColumn]
            (rf"(?<!['\w]){table_escaped}\s*\[\s*{old_escaped}\s*\]", f"{table_name}[{new_col_in_bracket}]", 0),
            # TMDL column definition: column OldName -> column NewName
            (rf'^(\s*)column\s+{old_escaped}\s*$', rf'\1column {new_name_quoted}', re.MULTILINE),
            (rf"^(\s*)column\s+'{old_escaped}'\s*$", rf'\1column {new_name_quoted}', re.MULTILINE),
            # fromColumn/toColumn with TableName.ColumnName format (CRITICAL for relationships)
            # fromColumn: TableName.OldColumn -> fromColumn: TableName.NewColumn
            (rf'(fromColumn\s*:\s*{table_escaped}\.)({old_escaped})(?=\s|$)', rf'\1{new_name_quoted}', re.MULTILINE),
            # fromColumn: TableName.'OldColumn' -> fromColumn: TableName.'NewColumn'
            (rf"(fromColumn\s*:\s*{table_escaped}\.)'{old_escaped}'", rf'\1{new_name_quoted}', 0),
            # fromColumn: 'TableName'.OldColumn -> fromColumn: 'TableName'.NewColumn
            (rf"(fromColumn\s*:\s*'{table_escaped}'\.)({old_escaped})(?=\s|$)", rf'\1{new_name_quoted}', re.MULTILINE),
            # fromColumn: 'TableName'.'OldColumn' -> fromColumn: 'TableName'.'NewColumn'
            (rf"(fromColumn\s*:\s*'{table_escaped}'\.)'{old_escaped}'", rf'\1{new_name_quoted}', 0),
            # toColumn: TableName.OldColumn -> toColumn: TableName.NewColumn
            (rf'(toColumn\s*:\s*{table_escaped}\.)({old_escaped})(?=\s|$)', rf'\1{new_name_quoted}', re.MULTILINE),
            # toColumn: TableName.'OldColumn' -> toColumn: TableName.'NewColumn'
            (rf"(toColumn\s*:\s*{table_escaped}\.)'{old_escaped}'", rf'\1{new_name_quoted}', 0),
            # toColumn: 'TableName'.OldColumn -> toColumn: 'TableName'.NewColumn
            (rf"(toColumn\s*:\s*'{table_escaped}'\.)({old_escaped})(?=\s|$)", rf'\1{new_name_quoted}', re.MULTILINE),
            # toColumn: 'TableName'.'OldColumn' -> toColumn: 'TableName'.'NewColumn'
            (rf"(toColumn\s*:\s*'{table_escaped}'\.)'{old_escaped}'", rf'\1{new_name_quoted}', 0),
        ]

        for tmdl_file in self.current_project.tmdl_files:
            try:
                self._cache_file_content(tmdl_file)

                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Column rename patterns are specific to DAX/TMDL syntax
                # They won't accidentally match M-Code patterns
                for pattern, replacement, flags in patterns:
                    content, count = re.subn(pattern, replacement, content, flags=flags)
                    file_count += count

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_count += file_count

            except Exception as e:
                logger.error(f"Error updating TMDL file {tmdl_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_measure_in_tmdl_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename measure references in TMDL files with proper quoting"""
        files_modified = []
        total_count = 0

        if not self.current_project or not self.current_project.tmdl_files:
            return {"files": files_modified, "count": total_count}

        new_name_quoted = quote_tmdl_name(new_name)
        old_escaped = re.escape(old_name)

        # Patterns for measure references
        patterns = [
            # [MeasureName] references in DAX
            (rf"\[\s*{old_escaped}\s*\]", f"[{new_name}]", 0),
            # TMDL measure definition: measure OldName = -> measure NewName =
            (rf'^(\s*)measure\s+{old_escaped}\s*=', rf'\1measure {new_name_quoted} =', re.MULTILINE),
            (rf"^(\s*)measure\s+'{old_escaped}'\s*=", rf'\1measure {new_name_quoted} =', re.MULTILINE),
        ]

        for tmdl_file in self.current_project.tmdl_files:
            try:
                self._cache_file_content(tmdl_file)

                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Measure rename patterns are specific to DAX/TMDL syntax
                # They won't accidentally match M-Code patterns
                for pattern, replacement, flags in patterns:
                    content, count = re.subn(pattern, replacement, content, flags=flags)
                    file_count += count

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_count += file_count

            except Exception as e:
                logger.error(f"Error updating TMDL file {tmdl_file}: {e}")

        return {"files": files_modified, "count": total_count}

    # ==================== PBIR VISUAL.JSON OPERATIONS ====================

    def _rename_table_in_visual_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in PBIR Enhanced format visual.json files.

        Each visual.json contains Entity references like:
        {
            "SourceRef": {
                "Entity": "Salesforce_Data"  <- This needs updating
            }
        }

        Returns:
            Dict with files modified and count of updates
        """
        if not self.current_project or not self.current_project.visual_json_files:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0

        for visual_file in self.current_project.visual_json_files:
            try:
                self._cache_file_content(visual_file)

                with open(visual_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Pattern 1: "Entity": "OldTableName"
                pattern1 = rf'"Entity"\s*:\s*"{re.escape(old_name)}"'
                replacement1 = f'"Entity": "{new_name}"'
                content, c = re.subn(pattern1, replacement1, content)
                file_count += c

                # Pattern 2: queryRef patterns like "OldTableName.ColumnName"
                pattern2 = rf'"queryRef"\s*:\s*"{re.escape(old_name)}\.([^"]+)"'
                replacement2 = rf'"queryRef": "{new_name}.\1"'
                content, c = re.subn(pattern2, replacement2, content)
                file_count += c

                # Pattern 3: nativeQueryRef with table prefix
                pattern3 = rf'"nativeQueryRef"\s*:\s*"{re.escape(old_name)}\.([^"]+)"'
                replacement3 = rf'"nativeQueryRef": "{new_name}.\1"'
                content, c = re.subn(pattern3, replacement3, content)
                file_count += c

                # Pattern 4: metadata selector patterns
                pattern4 = rf'"metadata"\s*:\s*"{re.escape(old_name)}\.([^"]+)"'
                replacement4 = rf'"metadata": "{new_name}.\1"'
                content, c = re.subn(pattern4, replacement4, content)
                file_count += c

                if content != original_content:
                    with open(visual_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(visual_file))
                    total_count += file_count
                    logger.info(f"Updated {file_count} Entity references in {visual_file.name}")

            except Exception as e:
                logger.error(f"Error updating visual file {visual_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_column_in_visual_files(self, table_name: str, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename column references in PBIR Enhanced format visual.json files"""
        if not self.current_project or not self.current_project.visual_json_files:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0

        for visual_file in self.current_project.visual_json_files:
            try:
                self._cache_file_content(visual_file)

                with open(visual_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Pattern 1: "Property": "ColumnName" (when Entity context is table_name)
                # This is tricky - we use JSON parsing for accuracy
                try:
                    visual_data = json.loads(content)
                    modified = self._deep_rename_column_in_json(visual_data, table_name, old_name, new_name)
                    if modified > 0:
                        content = json.dumps(visual_data, indent=2, ensure_ascii=False)
                        file_count += modified
                except json.JSONDecodeError:
                    pass

                # Pattern 2: queryRef patterns like "TableName.OldColumn"
                pattern2 = rf'"{re.escape(table_name)}\.{re.escape(old_name)}"'
                replacement2 = f'"{table_name}.{new_name}"'
                content, c = re.subn(pattern2, replacement2, content)
                file_count += c

                if content != original_content:
                    with open(visual_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(visual_file))
                    total_count += file_count

            except Exception as e:
                logger.error(f"Error updating visual file {visual_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_measure_in_visual_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename measure references in PBIR Enhanced format visual.json files"""
        if not self.current_project or not self.current_project.visual_json_files:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0

        for visual_file in self.current_project.visual_json_files:
            try:
                self._cache_file_content(visual_file)

                with open(visual_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Pattern 1: "Property": "MeasureName"
                pattern1 = rf'"Property"\s*:\s*"{re.escape(old_name)}"'
                replacement1 = f'"Property": "{new_name}"'
                content, c = re.subn(pattern1, replacement1, content)
                file_count += c

                # Pattern 2: queryRef ending with measure name
                pattern2 = rf'\.{re.escape(old_name)}"'
                replacement2 = f'.{new_name}"'
                content, c = re.subn(pattern2, replacement2, content)
                file_count += c

                # Pattern 3: nativeQueryRef with measure name
                pattern3 = rf'"nativeQueryRef"\s*:\s*"{re.escape(old_name)}"'
                replacement3 = f'"nativeQueryRef": "{new_name}"'
                content, c = re.subn(pattern3, replacement3, content)
                file_count += c

                if content != original_content:
                    with open(visual_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(visual_file))
                    total_count += file_count

            except Exception as e:
                logger.error(f"Error updating visual file {visual_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def fix_broken_visual_references(self, old_table_name: str, new_table_name: str) -> Dict[str, Any]:
        """
        Fix broken visual references after a table rename.

        This is a targeted fix for the common scenario where:
        - TOM/API renamed the table in the semantic model
        - But report visuals still reference the old table name

        Works with both PBIR-Legacy and PBIR-Enhanced formats.

        Args:
            old_table_name: The old table name that visuals are still referencing
            new_table_name: The correct new table name

        Returns:
            Dict with fix results
        """
        if not self.current_project:
            return {"success": False, "error": "No project loaded"}

        files_modified = []
        total_count = 0
        errors = []

        # Fix PBIR-Legacy report.json
        if self.current_project.report_json_path and not self.current_project.is_pbir_enhanced:
            result = self._rename_table_in_report_json(old_table_name, new_table_name)
            if result.get("count", 0) > 0:
                files_modified.append(str(self.current_project.report_json_path))
                total_count += result["count"]

        # Fix PBIR-Enhanced visual.json files
        if self.current_project.is_pbir_enhanced and self.current_project.visual_json_files:
            result = self._rename_table_in_visual_files(old_table_name, new_table_name)
            files_modified.extend(result.get("files", []))
            total_count += result.get("count", 0)

        # Also fix cultures files (linguistic schema)
        if self.current_project.cultures_files:
            result = self._rename_table_in_cultures_files(old_table_name, new_table_name)
            files_modified.extend(result.get("files", []))
            total_count += result.get("count", 0)

        # Also fix diagramLayout.json
        if self.current_project.diagram_layout_path:
            result = self._rename_table_in_diagram_layout(old_table_name, new_table_name)
            files_modified.extend(result.get("files", []))
            total_count += result.get("count", 0)

        return {
            "success": total_count > 0,
            "files_modified": files_modified,
            "references_fixed": total_count,
            "format": "PBIR-Enhanced" if self.current_project.is_pbir_enhanced else "PBIR-Legacy",
            "errors": errors
        }

    def _rename_table_in_cultures_files(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in cultures/linguistic schema files.

        These files contain:
        - "ConceptualEntity": "TableName" references used for Q&A
        - JSON keys like "TableName": { for linguistic entities
        """
        if not self.current_project or not self.current_project.cultures_files:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0

        for cultures_file in self.current_project.cultures_files:
            try:
                self._cache_file_content(cultures_file)

                with open(cultures_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                # Pattern 1: "ConceptualEntity": "TableName"
                pattern1 = rf'"ConceptualEntity"\s*:\s*"{re.escape(old_name)}"'
                replacement1 = f'"ConceptualEntity": "{new_name}"'
                content, c = re.subn(pattern1, replacement1, content)
                file_count += c

                # Pattern 2: JSON key "TableName": { (linguistic entity definition)
                pattern2 = rf'"{re.escape(old_name)}"\s*:\s*\{{'
                replacement2 = f'"{new_name}": {{'
                content, c = re.subn(pattern2, replacement2, content)
                file_count += c

                if content != original_content:
                    with open(cultures_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(cultures_file))
                    total_count += file_count
                    logger.info(f"Updated {file_count} linguistic schema references in {cultures_file.name}")

            except Exception as e:
                logger.error(f"Error updating cultures file {cultures_file}: {e}")

        return {"files": files_modified, "count": total_count}

    def _rename_table_in_diagram_layout(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in diagramLayout.json.

        This file contains nodeIndex properties that reference table names for the model diagram.
        """
        if not self.current_project or not self.current_project.diagram_layout_path:
            return {"files": [], "count": 0}

        files_modified = []
        total_count = 0
        diagram_file = self.current_project.diagram_layout_path

        try:
            self._cache_file_content(diagram_file)

            with open(diagram_file, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            file_count = 0

            # Pattern: "nodeIndex": "TableName"
            pattern = rf'"nodeIndex"\s*:\s*"{re.escape(old_name)}"'
            replacement = f'"nodeIndex": "{new_name}"'
            content, c = re.subn(pattern, replacement, content)
            file_count += c

            if content != original_content:
                with open(diagram_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                files_modified.append(str(diagram_file))
                total_count += file_count
                logger.info(f"Updated {file_count} nodeIndex references in diagramLayout.json")

        except Exception as e:
            logger.error(f"Error updating diagramLayout.json: {e}")

        return {"files": files_modified, "count": total_count}

    def scan_broken_references(self) -> Dict[str, Any]:
        """
        Scan the project for potentially broken references.

        Compares table names in semantic model vs references in report layer.

        Returns:
            Dict with broken references found
        """
        if not self.current_project:
            return {"error": "No project loaded"}

        # Get table names from semantic model
        model_tables = set()
        for tmdl_file in self.current_project.tmdl_files:
            try:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Find table declarations
                for match in re.finditer(r"^(?:\s*)table\s+'([^']+)'", content, re.MULTILINE):
                    model_tables.add(match.group(1).replace("''", "'"))
                for match in re.finditer(r"^(?:\s*)table\s+(\w+)\s*$", content, re.MULTILINE):
                    model_tables.add(match.group(1))
            except Exception:
                pass

        # Get table references from report layer
        report_tables = set()
        broken_refs = []

        # Check visual files (PBIR Enhanced)
        for visual_file in self.current_project.visual_json_files:
            try:
                with open(visual_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                for match in re.finditer(r'"Entity"\s*:\s*"([^"]+)"', content):
                    entity = match.group(1)
                    report_tables.add(entity)
                    if entity not in model_tables:
                        broken_refs.append({
                            "file": str(visual_file),
                            "entity": entity,
                            "type": "visual"
                        })
            except Exception:
                pass

        # Check report.json (PBIR Legacy)
        if self.current_project.report_json_path:
            try:
                with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                for match in re.finditer(r'"Entity"\s*:\s*"([^"]+)"', content):
                    entity = match.group(1)
                    report_tables.add(entity)
                    if entity not in model_tables:
                        broken_refs.append({
                            "file": str(self.current_project.report_json_path),
                            "entity": entity,
                            "type": "report.json"
                        })
            except Exception:
                pass

        # Find orphaned tables (in report but not in model)
        orphaned = report_tables - model_tables

        return {
            "model_tables": list(model_tables),
            "report_tables": list(report_tables),
            "broken_references": broken_refs,
            "orphaned_table_names": list(orphaned),
            "has_broken_refs": len(broken_refs) > 0
        }

    def fix_all_dax_quoting(self) -> Dict[str, Any]:
        """
        Fix all DAX expressions by properly quoting table names with spaces.

        Scans TMDL files for patterns like:
            Table Name[Column]  ->  'Table Name'[Column]
            COUNTROWS(Table Name)  ->  COUNTROWS('Table Name')

        Returns:
            Dict with count of fixes and files modified
        """
        if not self.current_project:
            return {"count": 0, "files_modified": [], "errors": [], "tables_fixed": []}

        files_modified = []
        total_count = 0
        errors = []
        tables_fixed = set()

        # First, find all table names with spaces
        tables_with_spaces = set()
        for tmdl_file in self.current_project.tmdl_files:
            try:
                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Find quoted table declarations: table 'Table Name'
                for match in re.finditer(r"^(?:\s*)table\s+'([^']+)'", content, re.MULTILINE):
                    name = match.group(1).replace("''", "'")
                    if ' ' in name:
                        tables_with_spaces.add(name)
            except Exception:
                pass

        if not tables_with_spaces:
            return {"count": 0, "files_modified": [], "errors": [], "tables_fixed": [], "message": "No tables with spaces found"}

        # Now fix unquoted references to these tables
        for tmdl_file in self.current_project.tmdl_files:
            try:
                self._cache_file_content(tmdl_file)

                with open(tmdl_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                original_content = content
                file_count = 0

                for table_name in tables_with_spaces:
                    # Pattern 1: Unquoted in column reference: Table Name[Column] -> 'Table Name'[Column]
                    # But NOT already quoted: 'Table Name'[Column]
                    pattern1 = rf"(?<!')({re.escape(table_name)})\[([^\]]+)\]"
                    replacement1 = rf"'{table_name}'[\2]"
                    content, c = re.subn(pattern1, replacement1, content)
                    if c > 0:
                        file_count += c
                        tables_fixed.add(table_name)

                    # Pattern 2: In COUNTROWS/SUMX etc: COUNTROWS(Table Name) -> COUNTROWS('Table Name')
                    funcs = ["COUNTROWS", "SUMX", "AVERAGEX", "MAXX", "MINX", "FILTER", "ALL", "ALLEXCEPT", "VALUES", "DISTINCT", "RELATEDTABLE"]
                    for func in funcs:
                        pattern2 = rf"({func}\s*\(\s*)({re.escape(table_name)})(\s*[,\)])"
                        replacement2 = rf"\1'{table_name}'\3"
                        content, c = re.subn(pattern2, replacement2, content, flags=re.IGNORECASE)
                        if c > 0:
                            file_count += c
                            tables_fixed.add(table_name)

                if content != original_content:
                    with open(tmdl_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    files_modified.append(str(tmdl_file))
                    total_count += file_count
                    logger.info(f"Fixed {file_count} quoting issues in {tmdl_file.name}")

            except Exception as e:
                errors.append({"file": str(tmdl_file), "error": str(e)})
                logger.error(f"Error fixing DAX quoting in {tmdl_file}: {e}")

        return {
            "count": total_count,
            "files_modified": files_modified,
            "tables_fixed": list(tables_fixed),
            "errors": errors
        }

    def _deep_rename_column_in_json(self, obj: Any, table_name: str, old_column: str, new_column: str) -> int:
        """
        Recursively traverse JSON to rename column references within a specific table context.

        Returns count of modifications made.
        """
        count = 0

        if isinstance(obj, dict):
            # Check if this is a column reference within our target table
            if obj.get("Entity") == table_name and obj.get("Property") == old_column:
                obj["Property"] = new_column
                count += 1

            # Recurse into dict values
            for key, value in obj.items():
                count += self._deep_rename_column_in_json(value, table_name, old_column, new_column)

        elif isinstance(obj, list):
            for item in obj:
                count += self._deep_rename_column_in_json(item, table_name, old_column, new_column)

        return count

    # ==================== REPORT.JSON OPERATIONS (PBIR-Legacy) ====================

    def _rename_table_in_report_json(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """
        Rename table references in report.json (PBIR-Legacy format)

        report.json contains visual field bindings like:
        - "Entity": "TableName"
        - "Property": "ColumnName"
        - queryRef patterns
        """
        if not self.current_project or not self.current_project.report_json_path:
            return {"count": 0}

        try:
            self._cache_file_content(self.current_project.report_json_path)

            with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            count = 0

            # Pattern 1: "Entity": "TableName" (exact match for Entity field)
            pattern1 = rf'"Entity"\s*:\s*"{re.escape(old_name)}"'
            replacement1 = f'"Entity": "{new_name}"'
            content, c = re.subn(pattern1, replacement1, content)
            count += c

            # Pattern 2: "Table": "TableName"
            pattern2 = rf'"Table"\s*:\s*"{re.escape(old_name)}"'
            replacement2 = f'"Table": "{new_name}"'
            content, c = re.subn(pattern2, replacement2, content)
            count += c

            # Pattern 3: queryRef with table name (e.g., "TableName.ColumnName")
            pattern3 = rf'"{re.escape(old_name)}\.([^"]+)"'
            replacement3 = rf'"{new_name}.\1"'
            content, c = re.subn(pattern3, replacement3, content)
            count += c

            # Pattern 4: NativeReferenceName with table
            pattern4 = rf'"NativeReferenceName"\s*:\s*"{re.escape(old_name)}"'
            replacement4 = f'"NativeReferenceName": "{new_name}"'
            content, c = re.subn(pattern4, replacement4, content)
            count += c

            if content != original_content:
                with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Updated {count} table references in report.json")

            return {"count": count}

        except Exception as e:
            logger.error(f"Error updating report.json: {e}")
            return {"count": 0, "error": str(e)}

    def _rename_column_in_report_json(self, table_name: str, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename column references in report.json"""
        if not self.current_project or not self.current_project.report_json_path:
            return {"count": 0}

        try:
            self._cache_file_content(self.current_project.report_json_path)

            with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            count = 0

            # Pattern 1: Table.Column pattern in queryRef
            pattern1 = rf'"{re.escape(table_name)}\.{re.escape(old_name)}"'
            replacement1 = f'"{table_name}.{new_name}"'
            content, c = re.subn(pattern1, replacement1, content)
            count += c

            # Pattern 2: Use JSON parsing for Property fields with Entity context
            try:
                report_data = json.loads(content)
                modified = self._deep_rename_column_in_json(report_data, table_name, old_name, new_name)
                if modified > 0:
                    content = json.dumps(report_data, indent=2, ensure_ascii=False)
                    count += modified
            except json.JSONDecodeError:
                # Fall back to regex if JSON parsing fails
                pass

            if content != original_content:
                with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Updated {count} column references in report.json")

            return {"count": count}

        except Exception as e:
            logger.error(f"Error updating report.json: {e}")
            return {"count": 0, "error": str(e)}

    def _deep_rename_column_in_json(self, obj: Any, table_name: str, old_name: str, new_name: str) -> int:
        """Recursively rename column references in JSON structure"""
        count = 0

        if isinstance(obj, dict):
            # Check if this dict has Entity=table_name and Property=old_name
            if obj.get("Entity") == table_name and obj.get("Property") == old_name:
                obj["Property"] = new_name
                count += 1

            # Also check for NativeReferenceName pattern
            if "NativeReferenceName" in obj:
                ref = obj["NativeReferenceName"]
                if isinstance(ref, str) and ref == f"{table_name}.{old_name}":
                    obj["NativeReferenceName"] = f"{table_name}.{new_name}"
                    count += 1

            # Recurse into nested dicts
            for key, value in obj.items():
                count += self._deep_rename_column_in_json(value, table_name, old_name, new_name)

        elif isinstance(obj, list):
            for item in obj:
                count += self._deep_rename_column_in_json(item, table_name, old_name, new_name)

        return count

    def _rename_measure_in_report_json(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename measure references in report.json"""
        if not self.current_project or not self.current_project.report_json_path:
            return {"count": 0}

        try:
            self._cache_file_content(self.current_project.report_json_path)

            with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            count = 0

            # Measures are referenced similarly to columns
            # Pattern: "Property": "MeasureName"
            pattern1 = rf'"Property"\s*:\s*"{re.escape(old_name)}"'
            replacement1 = f'"Property": "{new_name}"'
            content, c = re.subn(pattern1, replacement1, content)
            count += c

            # Pattern: Table.MeasureName in queryRef
            pattern2 = rf'\.{re.escape(old_name)}"'
            replacement2 = f'.{new_name}"'
            content, c = re.subn(pattern2, replacement2, content)
            count += c

            if content != original_content:
                with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Updated {count} measure references in report.json")

            return {"count": count}

        except Exception as e:
            logger.error(f"Error updating report.json: {e}")
            return {"count": 0, "error": str(e)}

    # ==================== BATCH OPERATIONS ====================

    def batch_rename_tables(self, renames: List[Dict[str, str]]) -> RenameResult:
        """
        Batch rename multiple tables in PBIP files

        Args:
            renames: List of {"old_name": "...", "new_name": "..."} dicts

        Returns:
            RenameResult with combined results
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        all_files: Set[str] = set()
        total_refs = 0
        results = []
        all_validation_errors: List[ValidationError] = []
        backup_path = None

        # Create backup before any changes
        if self.auto_backup:
            backup_path = self.create_backup()

        for rename in renames:
            old_name = rename.get("old_name")
            new_name = rename.get("new_name")

            if not old_name or not new_name:
                continue

            result = self.rename_table_in_files(old_name, new_name)
            results.append(result)
            all_files.update(result.files_modified)
            total_refs += result.references_updated
            all_validation_errors.extend(result.validation_errors)

        # Final validation
        final_validation = self.validate_tmdl_syntax()
        all_validation_errors.extend(final_validation)

        success = len(all_validation_errors) == 0

        message = f"Renamed {len(renames)} table(s) in PBIP files. Updated {total_refs} references in {len(all_files)} file(s)."
        if not success:
            message += f" WARNING: {len(all_validation_errors)} validation error(s) found!"

        return RenameResult(
            success=success,
            message=message,
            files_modified=list(all_files),
            references_updated=total_refs,
            details={"individual_results": [r.__dict__ for r in results]},
            validation_errors=all_validation_errors,
            backup_created=backup_path
        )

    def batch_rename_columns(self, renames: List[Dict[str, str]]) -> RenameResult:
        """
        Batch rename multiple columns in PBIP files

        Args:
            renames: List of {"table_name": "...", "old_name": "...", "new_name": "..."} dicts

        Returns:
            RenameResult with combined results
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        all_files: Set[str] = set()
        total_refs = 0
        results = []
        backup_path = None

        # Create backup before any changes
        if self.auto_backup:
            backup_path = self.create_backup()

        for rename in renames:
            table_name = rename.get("table_name")
            old_name = rename.get("old_name")
            new_name = rename.get("new_name")

            if not all([table_name, old_name, new_name]):
                continue

            result = self.rename_column_in_files(table_name, old_name, new_name)
            results.append(result)
            all_files.update(result.files_modified)
            total_refs += result.references_updated

        return RenameResult(
            success=True,
            message=f"Renamed {len(renames)} column(s) in PBIP files. Updated {total_refs} references in {len(all_files)} file(s).",
            files_modified=list(all_files),
            references_updated=total_refs,
            details={"individual_results": [r.__dict__ for r in results]},
            backup_created=backup_path
        )

    def batch_rename_measures(self, renames: List[Dict[str, str]]) -> RenameResult:
        """
        Batch rename multiple measures in PBIP files

        Args:
            renames: List of {"old_name": "...", "new_name": "..."} dicts

        Returns:
            RenameResult with combined results
        """
        if not self.current_project:
            return RenameResult(False, "No project loaded", [], 0)

        all_files: Set[str] = set()
        total_refs = 0
        results = []
        backup_path = None

        # Create backup before any changes
        if self.auto_backup:
            backup_path = self.create_backup()

        for rename in renames:
            old_name = rename.get("old_name")
            new_name = rename.get("new_name")

            if not old_name or not new_name:
                continue

            result = self.rename_measure_in_files(old_name, new_name)
            results.append(result)
            all_files.update(result.files_modified)
            total_refs += result.references_updated

        return RenameResult(
            success=True,
            message=f"Renamed {len(renames)} measure(s) in PBIP files. Updated {total_refs} references in {len(all_files)} file(s).",
            files_modified=list(all_files),
            references_updated=total_refs,
            details={"individual_results": [r.__dict__ for r in results]},
            backup_created=backup_path
        )

    # ==================== VISUAL / REPORT MANIPULATION ====================

    def get_visual_details(self, page_name: str, visual_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific visual including data bindings,
        filters, formatting, and configuration.

        Works with both PBIR-Enhanced and PBIR-Legacy formats.

        Args:
            page_name: Display name or ID of the page containing the visual
            visual_id: ID or name of the visual

        Returns:
            Dict with detailed visual information
        """
        if not self.current_project:
            return {"error": "No project loaded"}

        try:
            if self.current_project.is_pbir_enhanced:
                return self._get_visual_details_pbir_enhanced(page_name, visual_id)
            elif self.current_project.report_json_path:
                return self._get_visual_details_pbir_legacy(page_name, visual_id)
            else:
                return {"error": "No report found in project"}
        except Exception as e:
            logger.error(f"Error getting visual details: {e}")
            return {"error": str(e)}

    def _get_visual_details_pbir_enhanced(self, page_name: str, visual_id: str) -> Dict[str, Any]:
        """Get detailed visual info from PBIR-Enhanced format"""
        if not self.current_project or not self.current_project.pbir_definition_folder:
            return {"error": "No PBIR definition folder found"}

        pages_folder = self.current_project.pbir_definition_folder / "pages"
        if not pages_folder.exists():
            return {"error": "No pages folder found"}

        # Find the page by name or ID
        target_page_folder = None
        target_page_name = page_name

        for page_folder in pages_folder.iterdir():
            if not page_folder.is_dir():
                continue

            # Match by folder name (ID)
            if page_folder.name == page_name:
                target_page_folder = page_folder
                break

            # Match by displayName from page.json
            page_json_path = page_folder / "page.json"
            if page_json_path.exists():
                try:
                    with open(page_json_path, 'r', encoding='utf-8') as f:
                        page_data = json.load(f)
                    display_name = page_data.get("displayName", page_data.get("name", ""))
                    if display_name.lower() == page_name.lower():
                        target_page_folder = page_folder
                        target_page_name = display_name
                        break
                except Exception:
                    pass

        if not target_page_folder:
            return {"error": f"Page '{page_name}' not found"}

        # Find the visual
        visuals_folder = target_page_folder / "visuals"
        if not visuals_folder.exists():
            return {"error": f"No visuals folder found in page '{page_name}'"}

        target_visual_path = None
        for vf in visuals_folder.iterdir():
            if not vf.is_dir():
                continue
            if vf.name == visual_id:
                target_visual_path = vf / "visual.json"
                break

            # Also try matching by visual name inside visual.json
            vj = vf / "visual.json"
            if vj.exists():
                try:
                    with open(vj, 'r', encoding='utf-8') as f:
                        vdata = json.load(f)
                    vname = vdata.get("config", {}).get("name", "")
                    if vname == visual_id:
                        target_visual_path = vj
                        visual_id = vf.name
                        break
                except Exception:
                    pass

        if not target_visual_path or not target_visual_path.exists():
            return {"error": f"Visual '{visual_id}' not found on page '{page_name}'"}

        # Parse the full visual.json
        with open(target_visual_path, 'r', encoding='utf-8') as f:
            visual_data = json.load(f)

        result = {
            "success": True,
            "page_name": target_page_name,
            "visual_id": visual_id,
            "file_path": str(target_visual_path),
            "raw_config": visual_data,
        }

        # Extract structured info
        config = visual_data.get("config", {})
        result["visual_name"] = config.get("name", visual_id)
        result["visual_type"] = config.get("visualType", "Unknown")

        # Layouts / positioning
        layouts = config.get("layouts", [])
        if layouts:
            layout = layouts[0]
            result["position"] = {
                "x": layout.get("x", 0),
                "y": layout.get("y", 0),
                "width": layout.get("width", 0),
                "height": layout.get("height", 0),
                "z": layout.get("z", 0),
            }

        # Data bindings - extract from prototypeQuery
        prototype = config.get("prototypeQuery", {})
        if prototype:
            from_sources = prototype.get("From", [])
            select_items = prototype.get("Select", [])
            where_items = prototype.get("Where", [])

            result["data_sources"] = []
            for src in from_sources:
                result["data_sources"].append({
                    "name": src.get("Name", ""),
                    "entity": src.get("Entity", ""),
                    "type": src.get("Type", ""),
                })

            result["data_fields"] = []
            for sel in select_items:
                field_info = {}
                if "Column" in sel:
                    col = sel["Column"]
                    field_info = {
                        "type": "Column",
                        "expression": col.get("Expression", {}).get("SourceRef", {}).get("Source", ""),
                        "property": col.get("Property", ""),
                    }
                elif "Measure" in sel:
                    mea = sel["Measure"]
                    field_info = {
                        "type": "Measure",
                        "expression": mea.get("Expression", {}).get("SourceRef", {}).get("Source", ""),
                        "property": mea.get("Property", ""),
                    }
                elif "Aggregation" in sel:
                    agg = sel["Aggregation"]
                    field_info = {
                        "type": "Aggregation",
                        "function": agg.get("Function", ""),
                    }
                if field_info:
                    if "Name" in sel:
                        field_info["name"] = sel["Name"]
                    result["data_fields"].append(field_info)

            result["has_filters"] = len(where_items) > 0
            result["filter_count"] = len(where_items)

        # Visual-level filters
        if "filters" in visual_data:
            result["visual_filters"] = visual_data["filters"]

        return result

    def _get_visual_details_pbir_legacy(self, page_name: str, visual_id: str) -> Dict[str, Any]:
        """Get detailed visual info from PBIR-Legacy format"""
        if not self.current_project or not self.current_project.report_json_path:
            return {"error": "No report.json found"}

        with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        sections = report_data.get("sections", [])

        # Find the target section/page
        target_section = None
        for section in sections:
            display_name = section.get("displayName", section.get("name", ""))
            if display_name.lower() == page_name.lower() or section.get("name", "") == page_name:
                target_section = section
                break

        if not target_section:
            return {"error": f"Page '{page_name}' not found"}

        # Find the visual
        visual_containers = target_section.get("visualContainers", [])
        target_visual = None

        for vc in visual_containers:
            vc_config = vc.get("config", {})
            vc_name = vc_config.get("name", "")
            if vc_name == visual_id or vc.get("name", "") == visual_id:
                target_visual = vc
                break

        if not target_visual:
            return {"error": f"Visual '{visual_id}' not found on page '{page_name}'"}

        config = target_visual.get("config", {})

        result = {
            "success": True,
            "page_name": target_section.get("displayName", page_name),
            "visual_id": config.get("name", visual_id),
            "visual_name": config.get("name", visual_id),
            "visual_type": config.get("visualType", "Unknown"),
            "raw_config": target_visual,
        }

        # Layouts / positioning
        layouts = config.get("layouts", [])
        if layouts:
            layout = layouts[0]
            result["position"] = {
                "x": layout.get("x", 0),
                "y": layout.get("y", 0),
                "width": layout.get("width", 0),
                "height": layout.get("height", 0),
                "z": layout.get("z", 0),
            }

        # Query / data bindings
        query = config.get("query", {})
        if query:
            result["query"] = query

        # Data roles
        result["data_roles"] = config.get("dataRoles", [])

        # Filters
        if "filters" in target_visual:
            result["visual_filters"] = target_visual["filters"]

        return result

    def add_page(self, display_name: str, width: int = 1280, height: int = 720) -> Dict[str, Any]:
        """
        Add a new page to the report.

        Supports both PBIR-Enhanced and PBIR-Legacy formats.

        Args:
            display_name: Display name for the new page
            width: Page width in pixels (default: 1280)
            height: Page height in pixels (default: 720)

        Returns:
            Dict with page creation result
        """
        if not self.current_project:
            return {"error": "No project loaded"}

        try:
            if self.current_project.is_pbir_enhanced:
                return self._add_page_pbir_enhanced(display_name, width, height)
            elif self.current_project.report_json_path:
                return self._add_page_pbir_legacy(display_name, width, height)
            else:
                return {"error": "No report found in project"}
        except Exception as e:
            logger.error(f"Error adding page: {e}")
            return {"error": str(e)}

    def _add_page_pbir_enhanced(self, display_name: str, width: int, height: int) -> Dict[str, Any]:
        """Add a page in PBIR-Enhanced format"""
        import uuid

        if not self.current_project or not self.current_project.pbir_definition_folder:
            return {"error": "No PBIR definition folder found"}

        pages_folder = self.current_project.pbir_definition_folder / "pages"
        pages_folder.mkdir(parents=True, exist_ok=True)

        # Generate a unique page ID
        page_id = str(uuid.uuid4()).replace("-", "")[:20]

        # Create page folder and visuals subfolder
        page_folder = pages_folder / page_id
        page_folder.mkdir(parents=True, exist_ok=True)
        visuals_folder = page_folder / "visuals"
        visuals_folder.mkdir(parents=True, exist_ok=True)

        # Create page.json
        page_data = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.0.0/schema.json",
            "name": page_id,
            "displayName": display_name,
            "displayOption": "FitToPage",
            "height": height,
            "width": width,
        }

        page_json_path = page_folder / "page.json"
        with open(page_json_path, 'w', encoding='utf-8') as f:
            json.dump(page_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Created new page '{display_name}' with ID '{page_id}'")

        return {
            "success": True,
            "page_id": page_id,
            "display_name": display_name,
            "page_folder": str(page_folder),
            "width": width,
            "height": height,
        }

    def _add_page_pbir_legacy(self, display_name: str, width: int, height: int) -> Dict[str, Any]:
        """Add a page in PBIR-Legacy format"""
        import uuid

        if not self.current_project or not self.current_project.report_json_path:
            return {"error": "No report.json found"}

        self._cache_file_content(self.current_project.report_json_path)

        with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        sections = report_data.get("sections", [])
        page_id = f"Section_{str(uuid.uuid4()).replace('-', '')[:16]}"

        # Determine order (add at end)
        max_order = max((s.get("ordinal", 0) for s in sections), default=-1) + 1

        new_section = {
            "name": page_id,
            "displayName": display_name,
            "displayOption": 1,  # Fit to page
            "width": width,
            "height": height,
            "ordinal": max_order,
            "visualContainers": [],
        }

        sections.append(new_section)
        report_data["sections"] = sections

        with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Created new page '{display_name}' with ID '{page_id}'")

        return {
            "success": True,
            "page_id": page_id,
            "display_name": display_name,
            "width": width,
            "height": height,
        }

    def delete_page(self, page_name: str) -> Dict[str, Any]:
        """
        Delete a page from the report.

        Args:
            page_name: Display name or ID of the page to delete

        Returns:
            Dict with deletion result
        """
        if not self.current_project:
            return {"error": "No project loaded"}

        try:
            if self.current_project.is_pbir_enhanced:
                return self._delete_page_pbir_enhanced(page_name)
            elif self.current_project.report_json_path:
                return self._delete_page_pbir_legacy(page_name)
            else:
                return {"error": "No report found in project"}
        except Exception as e:
            logger.error(f"Error deleting page: {e}")
            return {"error": str(e)}

    def _delete_page_pbir_enhanced(self, page_name: str) -> Dict[str, Any]:
        """Delete a page in PBIR-Enhanced format"""
        if not self.current_project or not self.current_project.pbir_definition_folder:
            return {"error": "No PBIR definition folder found"}

        pages_folder = self.current_project.pbir_definition_folder / "pages"
        if not pages_folder.exists():
            return {"error": "No pages folder found"}

        target_folder = None
        target_display_name = page_name

        for page_folder in pages_folder.iterdir():
            if not page_folder.is_dir():
                continue

            if page_folder.name == page_name:
                target_folder = page_folder
                break

            page_json_path = page_folder / "page.json"
            if page_json_path.exists():
                try:
                    with open(page_json_path, 'r', encoding='utf-8') as f:
                        page_data = json.load(f)
                    dn = page_data.get("displayName", page_data.get("name", ""))
                    if dn.lower() == page_name.lower():
                        target_folder = page_folder
                        target_display_name = dn
                        break
                except Exception:
                    pass

        if not target_folder:
            return {"error": f"Page '{page_name}' not found"}

        # Remove the entire page folder (including all visuals)
        shutil.rmtree(target_folder)

        # Remove from visual_json_files cache
        self.current_project.visual_json_files = [
            v for v in self.current_project.visual_json_files
            if not str(v).startswith(str(target_folder))
        ]

        logger.info(f"Deleted page '{target_display_name}'")

        return {
            "success": True,
            "deleted_page": target_display_name,
            "deleted_folder": str(target_folder),
        }

    def _delete_page_pbir_legacy(self, page_name: str) -> Dict[str, Any]:
        """Delete a page in PBIR-Legacy format"""
        if not self.current_project or not self.current_project.report_json_path:
            return {"error": "No report.json found"}

        self._cache_file_content(self.current_project.report_json_path)

        with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        sections = report_data.get("sections", [])
        target_idx = None
        deleted_name = page_name

        for i, section in enumerate(sections):
            dn = section.get("displayName", section.get("name", ""))
            if dn.lower() == page_name.lower() or section.get("name", "") == page_name:
                target_idx = i
                deleted_name = dn
                break

        if target_idx is None:
            return {"error": f"Page '{page_name}' not found"}

        sections.pop(target_idx)
        report_data["sections"] = sections

        with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Deleted page '{deleted_name}'")

        return {
            "success": True,
            "deleted_page": deleted_name,
        }

    def add_visual(self, page_name: str, visual_type: str,
                   x: int = 0, y: int = 0, width: int = 400, height: int = 300,
                   table_name: str = "", column_name: str = "",
                   measure_name: str = "") -> Dict[str, Any]:
        """
        Add a new visual to a page.

        Supports both PBIR-Enhanced and PBIR-Legacy formats.

        Args:
            page_name: Display name or ID of the target page
            visual_type: Type of visual (e.g., "barChart", "lineChart", "tableEx",
                         "card", "slicer", "pieChart", "donutChart", "clusteredBarChart",
                         "clusteredColumnChart", "lineClusteredColumnComboChart",
                         "areaChart", "filledMap", "map", "treemap", "waterfallChart",
                         "funnel", "gauge", "multiRowCard", "kpi", "textbox", "image",
                         "shape", "actionButton")
            x: X position on canvas (default: 0)
            y: Y position on canvas (default: 0)
            width: Width in pixels (default: 400)
            height: Height in pixels (default: 300)
            table_name: Optional table name to bind data from
            column_name: Optional column name to bind (requires table_name)
            measure_name: Optional measure name to bind (requires table_name)

        Returns:
            Dict with visual creation result
        """
        if not self.current_project:
            return {"error": "No project loaded"}

        try:
            if self.current_project.is_pbir_enhanced:
                return self._add_visual_pbir_enhanced(
                    page_name, visual_type, x, y, width, height,
                    table_name, column_name, measure_name
                )
            elif self.current_project.report_json_path:
                return self._add_visual_pbir_legacy(
                    page_name, visual_type, x, y, width, height,
                    table_name, column_name, measure_name
                )
            else:
                return {"error": "No report found in project"}
        except Exception as e:
            logger.error(f"Error adding visual: {e}")
            return {"error": str(e)}

    def _build_visual_config(self, visual_id: str, visual_type: str,
                             x: int, y: int, width: int, height: int,
                             table_name: str = "", column_name: str = "",
                             measure_name: str = "") -> Dict[str, Any]:
        """Build a visual config dict for both PBIR formats"""
        config = {
            "name": visual_id,
            "visualType": visual_type,
            "layouts": [
                {
                    "x": x,
                    "y": y,
                    "width": width,
                    "height": height,
                    "z": 0,
                }
            ],
            "singleVisual": {
                "visualType": visual_type,
                "projections": {},
                "objects": {},
            },
        }

        # Add data bindings if table/column/measure are specified
        if table_name:
            source_alias = table_name[0].lower() if table_name else "t"

            from_clause = [{
                "Name": source_alias,
                "Entity": table_name,
                "Type": 0,
            }]

            select_clause = []

            if column_name:
                select_clause.append({
                    "Column": {
                        "Expression": {"SourceRef": {"Source": source_alias}},
                        "Property": column_name,
                    },
                    "Name": f"{table_name}.{column_name}",
                })

            if measure_name:
                select_clause.append({
                    "Measure": {
                        "Expression": {"SourceRef": {"Source": source_alias}},
                        "Property": measure_name,
                    },
                    "Name": f"{table_name}.{measure_name}",
                })

            if select_clause:
                config["prototypeQuery"] = {
                    "Version": 2,
                    "From": from_clause,
                    "Select": select_clause,
                }

        return config

    def _add_visual_pbir_enhanced(self, page_name: str, visual_type: str,
                                   x: int, y: int, width: int, height: int,
                                   table_name: str, column_name: str,
                                   measure_name: str) -> Dict[str, Any]:
        """Add a visual in PBIR-Enhanced format"""
        import uuid

        if not self.current_project or not self.current_project.pbir_definition_folder:
            return {"error": "No PBIR definition folder found"}

        pages_folder = self.current_project.pbir_definition_folder / "pages"
        if not pages_folder.exists():
            return {"error": "No pages folder found"}

        # Find the target page
        target_page_folder = None
        target_page_name = page_name

        for page_folder in pages_folder.iterdir():
            if not page_folder.is_dir():
                continue

            if page_folder.name == page_name:
                target_page_folder = page_folder
                break

            page_json_path = page_folder / "page.json"
            if page_json_path.exists():
                try:
                    with open(page_json_path, 'r', encoding='utf-8') as f:
                        page_data = json.load(f)
                    dn = page_data.get("displayName", page_data.get("name", ""))
                    if dn.lower() == page_name.lower():
                        target_page_folder = page_folder
                        target_page_name = dn
                        break
                except Exception:
                    pass

        if not target_page_folder:
            return {"error": f"Page '{page_name}' not found"}

        # Create the visual
        visual_id = str(uuid.uuid4()).replace("-", "")[:20]
        visual_folder = target_page_folder / "visuals" / visual_id
        visual_folder.mkdir(parents=True, exist_ok=True)

        visual_config = self._build_visual_config(
            visual_id, visual_type, x, y, width, height,
            table_name, column_name, measure_name
        )

        visual_data = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/1.0.0/schema.json",
            "config": visual_config,
        }

        visual_json_path = visual_folder / "visual.json"
        with open(visual_json_path, 'w', encoding='utf-8') as f:
            json.dump(visual_data, f, indent=2, ensure_ascii=False)

        # Update the cached visual files list
        self.current_project.visual_json_files.append(visual_json_path)

        logger.info(f"Added visual '{visual_type}' (ID: {visual_id}) to page '{target_page_name}'")

        return {
            "success": True,
            "visual_id": visual_id,
            "visual_type": visual_type,
            "page_name": target_page_name,
            "position": {"x": x, "y": y, "width": width, "height": height},
            "file_path": str(visual_json_path),
        }

    def _add_visual_pbir_legacy(self, page_name: str, visual_type: str,
                                 x: int, y: int, width: int, height: int,
                                 table_name: str, column_name: str,
                                 measure_name: str) -> Dict[str, Any]:
        """Add a visual in PBIR-Legacy format"""
        import uuid

        if not self.current_project or not self.current_project.report_json_path:
            return {"error": "No report.json found"}

        self._cache_file_content(self.current_project.report_json_path)

        with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        sections = report_data.get("sections", [])
        target_section = None

        for section in sections:
            dn = section.get("displayName", section.get("name", ""))
            if dn.lower() == page_name.lower() or section.get("name", "") == page_name:
                target_section = section
                break

        if not target_section:
            return {"error": f"Page '{page_name}' not found"}

        visual_id = str(uuid.uuid4()).replace("-", "")[:20]
        visual_config = self._build_visual_config(
            visual_id, visual_type, x, y, width, height,
            table_name, column_name, measure_name
        )

        new_container = {
            "name": visual_id,
            "config": visual_config,
            "x": x,
            "y": y,
            "width": width,
            "height": height,
        }

        if "visualContainers" not in target_section:
            target_section["visualContainers"] = []

        target_section["visualContainers"].append(new_container)

        with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        target_display = target_section.get("displayName", page_name)
        logger.info(f"Added visual '{visual_type}' (ID: {visual_id}) to page '{target_display}'")

        return {
            "success": True,
            "visual_id": visual_id,
            "visual_type": visual_type,
            "page_name": target_display,
            "position": {"x": x, "y": y, "width": width, "height": height},
        }

    def update_visual(self, page_name: str, visual_id: str,
                      visual_type: str = None,
                      x: int = None, y: int = None,
                      width: int = None, height: int = None,
                      table_name: str = None, column_name: str = None,
                      measure_name: str = None) -> Dict[str, Any]:
        """
        Update properties of an existing visual.

        Only non-None parameters will be updated.

        Args:
            page_name: Display name or ID of the page
            visual_id: ID or name of the visual to update
            visual_type: New visual type (optional)
            x: New X position (optional)
            y: New Y position (optional)
            width: New width (optional)
            height: New height (optional)
            table_name: New table binding (optional, clears old bindings)
            column_name: New column binding (optional, requires table_name)
            measure_name: New measure binding (optional, requires table_name)

        Returns:
            Dict with update result
        """
        if not self.current_project:
            return {"error": "No project loaded"}

        try:
            if self.current_project.is_pbir_enhanced:
                return self._update_visual_pbir_enhanced(
                    page_name, visual_id, visual_type, x, y, width, height,
                    table_name, column_name, measure_name
                )
            elif self.current_project.report_json_path:
                return self._update_visual_pbir_legacy(
                    page_name, visual_id, visual_type, x, y, width, height,
                    table_name, column_name, measure_name
                )
            else:
                return {"error": "No report found in project"}
        except Exception as e:
            logger.error(f"Error updating visual: {e}")
            return {"error": str(e)}

    def _update_visual_pbir_enhanced(self, page_name, visual_id, visual_type,
                                      x, y, width, height,
                                      table_name, column_name, measure_name):
        """Update a visual in PBIR-Enhanced format"""
        if not self.current_project or not self.current_project.pbir_definition_folder:
            return {"error": "No PBIR definition folder found"}

        pages_folder = self.current_project.pbir_definition_folder / "pages"

        # Find the page and visual JSON file
        target_visual_path = None
        target_page_name = page_name

        for page_folder in pages_folder.iterdir():
            if not page_folder.is_dir():
                continue

            matched = page_folder.name == page_name
            if not matched:
                pj = page_folder / "page.json"
                if pj.exists():
                    try:
                        with open(pj, 'r', encoding='utf-8') as f:
                            pd = json.load(f)
                        dn = pd.get("displayName", pd.get("name", ""))
                        if dn.lower() == page_name.lower():
                            matched = True
                            target_page_name = dn
                    except Exception:
                        pass

            if matched:
                visuals_folder = page_folder / "visuals"
                if visuals_folder.exists():
                    for vf in visuals_folder.iterdir():
                        if not vf.is_dir():
                            continue
                        if vf.name == visual_id:
                            target_visual_path = vf / "visual.json"
                            break
                        vj = vf / "visual.json"
                        if vj.exists():
                            try:
                                with open(vj, 'r', encoding='utf-8') as f:
                                    vd = json.load(f)
                                if vd.get("config", {}).get("name", "") == visual_id:
                                    target_visual_path = vj
                                    visual_id = vf.name
                                    break
                            except Exception:
                                pass
                break

        if not target_visual_path or not target_visual_path.exists():
            return {"error": f"Visual '{visual_id}' not found on page '{page_name}'"}

        self._cache_file_content(target_visual_path)

        with open(target_visual_path, 'r', encoding='utf-8') as f:
            visual_data = json.load(f)

        config = visual_data.get("config", {})
        changes = []

        # Update visual type
        if visual_type is not None:
            config["visualType"] = visual_type
            if "singleVisual" in config:
                config["singleVisual"]["visualType"] = visual_type
            changes.append(f"type -> {visual_type}")

        # Update layout/position
        layouts = config.get("layouts", [{}])
        if not layouts:
            layouts = [{}]
        layout = layouts[0]

        if x is not None:
            layout["x"] = x
            changes.append(f"x -> {x}")
        if y is not None:
            layout["y"] = y
            changes.append(f"y -> {y}")
        if width is not None:
            layout["width"] = width
            changes.append(f"width -> {width}")
        if height is not None:
            layout["height"] = height
            changes.append(f"height -> {height}")

        config["layouts"] = [layout]

        # Update data bindings
        if table_name is not None:
            source_alias = table_name[0].lower() if table_name else "t"
            from_clause = [{"Name": source_alias, "Entity": table_name, "Type": 0}]
            select_clause = []

            if column_name:
                select_clause.append({
                    "Column": {
                        "Expression": {"SourceRef": {"Source": source_alias}},
                        "Property": column_name,
                    },
                    "Name": f"{table_name}.{column_name}",
                })
            if measure_name:
                select_clause.append({
                    "Measure": {
                        "Expression": {"SourceRef": {"Source": source_alias}},
                        "Property": measure_name,
                    },
                    "Name": f"{table_name}.{measure_name}",
                })

            if select_clause:
                config["prototypeQuery"] = {
                    "Version": 2,
                    "From": from_clause,
                    "Select": select_clause,
                }
                changes.append(f"data binding -> {table_name}")

        visual_data["config"] = config

        with open(target_visual_path, 'w', encoding='utf-8') as f:
            json.dump(visual_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Updated visual '{visual_id}' on page '{target_page_name}': {', '.join(changes)}")

        return {
            "success": True,
            "visual_id": visual_id,
            "page_name": target_page_name,
            "changes": changes,
            "file_path": str(target_visual_path),
        }

    def _update_visual_pbir_legacy(self, page_name, visual_id, visual_type,
                                    x, y, width, height,
                                    table_name, column_name, measure_name):
        """Update a visual in PBIR-Legacy format"""
        if not self.current_project or not self.current_project.report_json_path:
            return {"error": "No report.json found"}

        self._cache_file_content(self.current_project.report_json_path)

        with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        sections = report_data.get("sections", [])
        target_visual = None
        target_section = None

        for section in sections:
            dn = section.get("displayName", section.get("name", ""))
            if dn.lower() == page_name.lower() or section.get("name", "") == page_name:
                target_section = section
                for vc in section.get("visualContainers", []):
                    vc_config = vc.get("config", {})
                    if vc_config.get("name", "") == visual_id or vc.get("name", "") == visual_id:
                        target_visual = vc
                        break
                break

        if not target_visual:
            return {"error": f"Visual '{visual_id}' not found on page '{page_name}'"}

        config = target_visual.get("config", {})
        changes = []

        if visual_type is not None:
            config["visualType"] = visual_type
            changes.append(f"type -> {visual_type}")

        layouts = config.get("layouts", [{}])
        if not layouts:
            layouts = [{}]
        layout = layouts[0]

        if x is not None:
            layout["x"] = x
            target_visual["x"] = x
            changes.append(f"x -> {x}")
        if y is not None:
            layout["y"] = y
            target_visual["y"] = y
            changes.append(f"y -> {y}")
        if width is not None:
            layout["width"] = width
            target_visual["width"] = width
            changes.append(f"width -> {width}")
        if height is not None:
            layout["height"] = height
            target_visual["height"] = height
            changes.append(f"height -> {height}")

        config["layouts"] = [layout]
        target_visual["config"] = config

        with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        page_display = target_section.get("displayName", page_name) if target_section else page_name
        logger.info(f"Updated visual '{visual_id}' on page '{page_display}': {', '.join(changes)}")

        return {
            "success": True,
            "visual_id": visual_id,
            "page_name": page_display,
            "changes": changes,
        }

    def delete_visual(self, page_name: str, visual_id: str) -> Dict[str, Any]:
        """
        Delete a visual from a page.

        Args:
            page_name: Display name or ID of the page
            visual_id: ID or name of the visual to delete

        Returns:
            Dict with deletion result
        """
        if not self.current_project:
            return {"error": "No project loaded"}

        try:
            if self.current_project.is_pbir_enhanced:
                return self._delete_visual_pbir_enhanced(page_name, visual_id)
            elif self.current_project.report_json_path:
                return self._delete_visual_pbir_legacy(page_name, visual_id)
            else:
                return {"error": "No report found in project"}
        except Exception as e:
            logger.error(f"Error deleting visual: {e}")
            return {"error": str(e)}

    def _delete_visual_pbir_enhanced(self, page_name: str, visual_id: str) -> Dict[str, Any]:
        """Delete a visual in PBIR-Enhanced format"""
        if not self.current_project or not self.current_project.pbir_definition_folder:
            return {"error": "No PBIR definition folder found"}

        pages_folder = self.current_project.pbir_definition_folder / "pages"

        target_visual_folder = None
        target_page_name = page_name

        for page_folder in pages_folder.iterdir():
            if not page_folder.is_dir():
                continue

            matched = page_folder.name == page_name
            if not matched:
                pj = page_folder / "page.json"
                if pj.exists():
                    try:
                        with open(pj, 'r', encoding='utf-8') as f:
                            pd = json.load(f)
                        dn = pd.get("displayName", pd.get("name", ""))
                        if dn.lower() == page_name.lower():
                            matched = True
                            target_page_name = dn
                    except Exception:
                        pass

            if matched:
                visuals_folder = page_folder / "visuals"
                if visuals_folder.exists():
                    for vf in visuals_folder.iterdir():
                        if not vf.is_dir():
                            continue
                        if vf.name == visual_id:
                            target_visual_folder = vf
                            break
                        vj = vf / "visual.json"
                        if vj.exists():
                            try:
                                with open(vj, 'r', encoding='utf-8') as f:
                                    vd = json.load(f)
                                if vd.get("config", {}).get("name", "") == visual_id:
                                    target_visual_folder = vf
                                    break
                            except Exception:
                                pass
                break

        if not target_visual_folder:
            return {"error": f"Visual '{visual_id}' not found on page '{page_name}'"}

        shutil.rmtree(target_visual_folder)

        # Update cached visual files
        self.current_project.visual_json_files = [
            v for v in self.current_project.visual_json_files
            if not str(v).startswith(str(target_visual_folder))
        ]

        logger.info(f"Deleted visual '{visual_id}' from page '{target_page_name}'")

        return {
            "success": True,
            "deleted_visual_id": visual_id,
            "page_name": target_page_name,
        }

    def _delete_visual_pbir_legacy(self, page_name: str, visual_id: str) -> Dict[str, Any]:
        """Delete a visual in PBIR-Legacy format"""
        if not self.current_project or not self.current_project.report_json_path:
            return {"error": "No report.json found"}

        self._cache_file_content(self.current_project.report_json_path)

        with open(self.current_project.report_json_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        sections = report_data.get("sections", [])
        found = False
        page_display = page_name

        for section in sections:
            dn = section.get("displayName", section.get("name", ""))
            if dn.lower() == page_name.lower() or section.get("name", "") == page_name:
                page_display = dn
                containers = section.get("visualContainers", [])
                for i, vc in enumerate(containers):
                    vc_config = vc.get("config", {})
                    if vc_config.get("name", "") == visual_id or vc.get("name", "") == visual_id:
                        containers.pop(i)
                        found = True
                        break
                section["visualContainers"] = containers
                break

        if not found:
            return {"error": f"Visual '{visual_id}' not found on page '{page_name}'"}

        with open(self.current_project.report_json_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Deleted visual '{visual_id}' from page '{page_display}'")

        return {
            "success": True,
            "deleted_visual_id": visual_id,
            "page_name": page_display,
        }

