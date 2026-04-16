"""
Power BI REST API Connector
For listing workspaces and datasets from Power BI Service

Supports:
- Service Principal auth (client_id + client_secret)
- Device Flow auth (interactive login via browser, works with MFA)
"""
import logging
import sys
import threading
from typing import Any, Dict, List, Optional
import requests
import msal

logger = logging.getLogger(__name__)


class PowerBIRestConnector:
    """Power BI connector using REST API for workspace/dataset listing"""

    BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
    # For service principal (client credentials) use /.default
    SCOPE_CLIENT = ["https://analysis.windows.net/powerbi/api/.default"]
    # For delegated/user flows use specific scopes
    SCOPE_USER = ["https://analysis.windows.net/powerbi/api/Dataset.Read.All",
                   "https://analysis.windows.net/powerbi/api/Workspace.Read.All",
                   "https://analysis.windows.net/powerbi/api/Report.Read.All"]

    def __init__(self, tenant_id: str, client_id: str, client_secret: str = "",
                 auth_mode: str = "service_principal"):
        """Initialize connector with Azure AD credentials

        Args:
            tenant_id: Azure AD tenant ID
            client_id: App Registration client ID
            client_secret: App client secret (only for service_principal mode)
            auth_mode: 'service_principal' or 'device_flow'
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_mode = auth_mode
        self.access_token = None
        self._msal_app = None
        self._accounts = []

    def _get_msal_app(self):
        """Get or create the MSAL application"""
        if self._msal_app is None:
            authority_url = self.AUTHORITY.format(tenant_id=self.tenant_id)
            if self.auth_mode == "device_flow":
                self._msal_app = msal.PublicClientApplication(
                    self.client_id,
                    authority=authority_url,
                )
            else:
                self._msal_app = msal.ConfidentialClientApplication(
                    self.client_id,
                    authority=authority_url,
                    client_credential=self.client_secret,
                )
        return self._msal_app

    def authenticate(self) -> bool:
        """Authenticate and get access token"""
        if self.auth_mode == "device_flow":
            return self._authenticate_device_flow()
        else:
            return self._authenticate_service_principal()

    def _authenticate_service_principal(self) -> bool:
        """Authenticate using Service Principal and get access token"""
        try:
            app = self._get_msal_app()
            result = app.acquire_token_for_client(scopes=self.SCOPE_CLIENT)

            if "access_token" in result:
                self.access_token = result["access_token"]
                logger.info("Successfully authenticated to Power BI Service (service_principal)")
                return True
            else:
                error = result.get("error_description", "Unknown error")
                logger.error(f"Authentication failed: {error}")
                return False

        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False

    def _authenticate_device_flow(self) -> bool:
        """Authenticate using OAuth Device Flow (works with MFA)

        Prints a device code + URL to stderr for the user to visit.
        Blocks until the user completes login or timeout.
        """
        try:
            app = self._get_msal_app()

            # Try to silently acquire from MSAL cache first
            accounts = app.get_accounts()
            if accounts:
                result = app.acquire_token_silent(self.SCOPE_USER, account=accounts[0])
                if result and "access_token" in result:
                    self.access_token = result["access_token"]
                    logger.info("Authenticated via cached token (device_flow)")
                    return True

            # No cached token — initiate device flow
            flow = app.initiate_device_flow(scopes=self.SCOPE_USER)
            if "user_code" not in flow:
                logger.error(f"Device flow initiation failed: {flow.get('error_description', 'Unknown')}")
                return False

            # Print instructions to stderr (visible to the user in the MCP server logs)
            msg = (
                "\n"
                "╔══════════════════════════════════════════════════════════╗\n"
                "║           Power BI — Device Flow Authentication         ║\n"
                "╠══════════════════════════════════════════════════════════╣\n"
                f"║  1. Open:  {flow['verification_uri']:<44s}║\n"
                f"║  2. Enter code:  {flow['user_code']:<38s}║\n"
                "║  3. Sign in with your Power BI account                  ║\n"
                "║                                                         ║\n"
                f"║  ⏳ Waiting (expires in {flow.get('expires_in', 900)}s)...                        ║\n"
                "╚══════════════════════════════════════════════════════════╝\n"
            )
            sys.stderr.write(msg)
            sys.stderr.flush()

            # Block until user completes login (or timeout)
            result = app.acquire_token_by_device_flow(flow)

            if "access_token" in result:
                self.access_token = result["access_token"]
                self._accounts = app.get_accounts()
                user = result.get("id_token_claims", {}).get("preferred_username", "unknown")
                logger.info(f"✅ Device flow authentication successful — logged in as {user}")
                sys.stderr.write(f"✅ Authenticated as: {user}\n")
                sys.stderr.flush()
                return True
            else:
                error = result.get("error_description", result.get("error", "Unknown error"))
                logger.error(f"Device flow authentication failed: {error}")
                sys.stderr.write(f"❌ Authentication failed: {error}\n")
                sys.stderr.flush()
                return False

        except Exception as e:
            logger.error(f"Device flow authentication error: {str(e)}")
            return False

    def get_access_token(self) -> Optional[str]:
        """Get current access token (authenticate if needed).
        
        Useful for sharing the token with other connectors (e.g. XMLA).
        """
        if not self.access_token:
            self.authenticate()
        return self.access_token

    def refresh_token_if_needed(self) -> bool:
        """Silently refresh token using MSAL cache"""
        if self.auth_mode != "device_flow":
            return self.authenticate()

        app = self._get_msal_app()
        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(self.SCOPE_USER, account=accounts[0])
            if result and "access_token" in result:
                self.access_token = result["access_token"]
                return True

        # Cache miss — need interactive flow again
        return self._authenticate_device_flow()

    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with authorization"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """
        List all workspaces accessible by the authenticated identity
        """
        try:
            if not self.access_token:
                if not self.authenticate():
                    return []

            url = f"{self.BASE_URL}/groups"
            response = requests.get(url, headers=self._get_headers(), timeout=30)

            # If 401, try refreshing the token once
            if response.status_code == 401:
                if self.refresh_token_if_needed():
                    response = requests.get(url, headers=self._get_headers(), timeout=30)

            response.raise_for_status()

            workspaces = response.json().get("value", [])
            logger.info(f"Found {len(workspaces)} workspace(s)")

            return [
                {
                    "id": ws["id"],
                    "name": ws["name"],
                    "type": ws.get("type", "Workspace"),
                    "state": ws.get("state", "Active"),
                }
                for ws in workspaces
            ]

        except Exception as e:
            logger.error(f"Failed to list workspaces: {str(e)}")
            return []

    def list_datasets(self, workspace_id: str) -> List[Dict[str, Any]]:
        """
        List all datasets in a workspace
        """
        try:
            if not self.access_token:
                if not self.authenticate():
                    return []

            url = f"{self.BASE_URL}/groups/{workspace_id}/datasets"
            response = requests.get(url, headers=self._get_headers(), timeout=30)

            # If 401, try refreshing the token once
            if response.status_code == 401:
                if self.refresh_token_if_needed():
                    response = requests.get(url, headers=self._get_headers(), timeout=30)

            response.raise_for_status()

            datasets = response.json().get("value", [])
            logger.info(f"Found {len(datasets)} dataset(s)")

            return [
                {
                    "id": ds["id"],
                    "name": ds["name"],
                    "configuredBy": ds.get("configuredBy", "Unknown"),
                    "isRefreshable": ds.get("isRefreshable", False),
                }
                for ds in datasets
            ]

        except Exception as e:
            logger.error(f"Failed to list datasets: {str(e)}")
            return []

    def list_reports(self, workspace_id: str) -> List[Dict[str, Any]]:
        """
        List all reports in a workspace

        Args:
            workspace_id: ID of the workspace

        Returns:
            List of report info dicts
        """
        try:
            if not self.access_token:
                if not self.authenticate():
                    return []

            url = f"{self.BASE_URL}/groups/{workspace_id}/reports"
            response = requests.get(url, headers=self._get_headers(), timeout=30)

            # If 401, try refreshing the token once
            if response.status_code == 401:
                if self.refresh_token_if_needed():
                    response = requests.get(url, headers=self._get_headers(), timeout=30)

            response.raise_for_status()

            reports = response.json().get("value", [])
            logger.info(f"Found {len(reports)} report(s)")

            return [
                {
                    "id": r["id"],
                    "name": r["name"],
                    "reportType": r.get("reportType", "Unknown"),
                    "datasetId": r.get("datasetId", ""),
                    "webUrl": r.get("webUrl", ""),
                    "embedUrl": r.get("embedUrl", ""),
                }
                for r in reports
            ]

        except Exception as e:
            logger.error(f"Failed to list reports: {str(e)}")
            return []

    def get_report_pages(self, workspace_id: str, report_id: str) -> List[Dict[str, Any]]:
        """
        Get pages of a specific report

        Args:
            workspace_id: ID of the workspace
            report_id: ID of the report

        Returns:
            List of page info dicts
        """
        try:
            if not self.access_token:
                if not self.authenticate():
                    return []

            url = f"{self.BASE_URL}/groups/{workspace_id}/reports/{report_id}/pages"
            response = requests.get(url, headers=self._get_headers(), timeout=30)

            if response.status_code == 401:
                if self.refresh_token_if_needed():
                    response = requests.get(url, headers=self._get_headers(), timeout=30)

            response.raise_for_status()

            pages = response.json().get("value", [])
            logger.info(f"Found {len(pages)} page(s)")

            return [
                {
                    "name": p.get("name", ""),
                    "displayName": p.get("displayName", ""),
                    "order": p.get("order", 0),
                }
                for p in pages
            ]

        except Exception as e:
            logger.error(f"Failed to get report pages: {str(e)}")
            return []

    def get_page_visuals(self, workspace_id: str, report_id: str, page_name: str) -> List[Dict[str, Any]]:
        """
        Get visuals on a specific page of a report

        Args:
            workspace_id: ID of the workspace
            report_id: ID of the report
            page_name: Internal name of the page (from get_report_pages)

        Returns:
            List of visual info dicts
        """
        try:
            if not self.access_token:
                if not self.authenticate():
                    return []

            url = f"{self.BASE_URL}/groups/{workspace_id}/reports/{report_id}/pages/{page_name}/visuals"
            response = requests.get(url, headers=self._get_headers(), timeout=30)

            if response.status_code == 401:
                if self.refresh_token_if_needed():
                    response = requests.get(url, headers=self._get_headers(), timeout=30)

            response.raise_for_status()

            visuals = response.json().get("value", [])
            logger.info(f"Found {len(visuals)} visual(s)")

            return [
                {
                    "name": v.get("name", ""),
                    "title": v.get("title", ""),
                    "type": v.get("type", "Unknown"),
                    "layout": v.get("layout", {}),
                }
                for v in visuals
            ]

        except Exception as e:
            logger.error(f"Failed to get page visuals: {str(e)}")
            return []
