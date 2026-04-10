import json
import subprocess
import sys

ROLE_ID = sys.argv[1]
DIRECTORY_OBJECT_ID = sys.argv[2]
AZ = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"

body = json.dumps({
    "@odata.id": f"https://graph.microsoft.com/v1.0/directoryObjects/{DIRECTORY_OBJECT_ID}"
})

result = subprocess.run(
    [
        AZ,
        "rest",
        "--method",
        "post",
        "--headers",
        "Content-Type=application/json",
        "--url",
        f"https://graph.microsoft.com/v1.0/directoryRoles/{ROLE_ID}/members/$ref",
        "--body",
        body,
    ],
    text=True,
    capture_output=True,
)

if result.stdout:
    print(result.stdout)
if result.stderr:
    print(result.stderr)

raise SystemExit(result.returncode)
