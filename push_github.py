import os
import subprocess

token = os.environ.get("GITHUB_TOKEN", "")
if not token:
    print("ERROR: GITHUB_TOKEN not set")
    exit(1)

remote = f"https://{token}@github.com/AMANITAPOWER999/goldantelope7.git"

cmds = [
    ["git", "config", "user.email", "bot@goldantelope.app"],
    ["git", "config", "user.name", "GoldAntelope Bot"],
    ["git", "add", "-A"],
]

for cmd in cmds:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"ERR {cmd}: {r.stderr}")
    else:
        print(f"OK {cmd[1]}: {r.stdout.strip()}")

# Commit
r = subprocess.run(
    ["git", "commit", "-m", "Eager loading for all slider images"],
    capture_output=True, text=True
)
print("Commit:", r.stdout.strip() or r.stderr.strip())

# Push
r = subprocess.run(
    ["git", "push", remote, "master"],
    capture_output=True, text=True
)
print("Push stdout:", r.stdout.strip())
print("Push stderr:", r.stderr.strip())
print("Push returncode:", r.returncode)
