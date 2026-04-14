import clr
import sys

sys.path.append(r"C:\Program Files\Microsoft.NET\ADOMD.NET\160")

clr.AddReference("Microsoft.AnalysisServices.AdomdClient")

from Microsoft.AnalysisServices.AdomdClient import AdomdConnection

print("ADOMD LOADED SUCCESSFULLY")