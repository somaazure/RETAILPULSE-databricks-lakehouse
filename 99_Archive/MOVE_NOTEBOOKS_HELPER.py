# Databricks notebook source
# DBTITLE 1,Notebook Organization Helper
# MAGIC %md
# MAGIC # RetailPulse Notebook Organization
# MAGIC
# MAGIC **Purpose**: Move all framework notebooks to the new organized folder structure
# MAGIC
# MAGIC ## New Folder Structure
# MAGIC
# MAGIC ```
# MAGIC RetailPulse/
# MAGIC ├── 00_Setup/ (One-time configuration)
# MAGIC │   ├── 01_Setup_Audit_Tables
# MAGIC │   └── 02_Metadata_Configuration
# MAGIC ├── 01_DQ_Framework/ (Daily data quality)
# MAGIC │   └── 03_DQ_Framework
# MAGIC ├── 02_Maintenance/ (Weekly maintenance)
# MAGIC │   └── 04_Maintenance_Framework
# MAGIC ├── 03_Reporting/ (Ad-hoc reporting)
# MAGIC │   └── 05_Audit_Reporting
# MAGIC ├── 04_Orchestration/ (Job configs)
# MAGIC ├── 05_Documentation/ (Architecture docs)
# MAGIC │   └── RetailPulse Framework Architecture
# MAGIC └── 99_Archive/ (Future use)
# MAGIC ```
# MAGIC
# MAGIC **Run the cell below to move all notebooks**

# COMMAND ----------

# DBTITLE 1,Discover Actual Notebook Locations
import os
import glob

print("🔍 Discovering actual notebook file locations...\n")
print("=" * 80)

base_path = "/Workspace/Users/shekartelstra@gmail.com"

# Check RetailPulse/notebooks/ folder
notebooks_path = f"{base_path}/RetailPulse/notebooks"
print(f"\n📁 Checking: {notebooks_path}")
if os.path.exists(notebooks_path):
    files = os.listdir(notebooks_path)
    print(f"   Found {len(files)} items:")
    for f in files:
        full_path = os.path.join(notebooks_path, f)
        file_type = "📁 DIR " if os.path.isdir(full_path) else "📄 FILE"
        print(f"   {file_type} {f}")
else:
    print("   ⚠️  Folder doesn't exist")

# Check root RetailPulse folder
retail_path = f"{base_path}/RetailPulse"
print(f"\n📁 Checking: {retail_path}")
if os.path.exists(retail_path):
    files = [f for f in os.listdir(retail_path) if not f.startswith('.')]
    print(f"   Found {len(files)} items:")
    for f in sorted(files):
        full_path = os.path.join(retail_path, f)
        file_type = "📁 DIR " if os.path.isdir(full_path) else "📄 FILE"
        print(f"   {file_type} {f}")
else:
    print("   ⚠️  Folder doesn't exist")

# Check user home directory for loose notebooks
print(f"\n📁 Checking user home: {base_path}")
notebook_files = glob.glob(f"{base_path}/*.ipynb")
print(f"   Found {len(notebook_files)} .ipynb files:")
for nb in notebook_files:
    print(f"   📄 {os.path.basename(nb)}")

print("\n" + "=" * 80)
print("\n💡 Tip: Notebooks are stored as .ipynb files in the file system!")
print("   Example: '01_Setup_Audit_Tables.ipynb' not '01_Setup_Audit_Tables'")
print("\n" + "=" * 80)

# COMMAND ----------

# DBTITLE 1,Move Notebooks to New Structure
import os
import subprocess

base_path = "/Workspace/Users/shekartelstra@gmail.com/RetailPulse"

# Define source and target paths for notebooks (WITH .ipynb extension!)
moves = [
    {
        "source": f"{base_path}/notebooks/01_Setup_Audit_Tables.ipynb",
        "target": f"{base_path}/00_Setup/01_Setup_Audit_Tables.ipynb",
        "name": "01_Setup_Audit_Tables",
        "folder": "00_Setup"
    },
    {
        "source": f"{base_path}/notebooks/02_Metadata_Configuration.ipynb",
        "target": f"{base_path}/00_Setup/02_Metadata_Configuration.ipynb",
        "name": "02_Metadata_Configuration",
        "folder": "00_Setup"
    },
    {
        "source": f"{base_path}/notebooks/03_DQ_Framework.ipynb",
        "target": f"{base_path}/01_DQ_Framework/03_DQ_Framework.ipynb",
        "name": "03_DQ_Framework",
        "folder": "01_DQ_Framework"
    },
    {
        "source": f"{base_path}/notebooks/04_Maintenance_Framework.ipynb",
        "target": f"{base_path}/02_Maintenance/04_Maintenance_Framework.ipynb",
        "name": "04_Maintenance_Framework",
        "folder": "02_Maintenance"
    },
    {
        "source": f"{base_path}/notebooks/05_Audit_Reporting.ipynb",
        "target": f"{base_path}/03_Reporting/05_Audit_Reporting.ipynb",
        "name": "05_Audit_Reporting",
        "folder": "03_Reporting"
    },
    {
        "source": "/Workspace/Users/shekartelstra@gmail.com/RetailPulse Framework Architecture.ipynb",
        "target": f"{base_path}/05_Documentation/RetailPulse Framework Architecture.ipynb",
        "name": "RetailPulse Framework Architecture",
        "folder": "05_Documentation"
    }
]

print("📦 Moving notebooks to organized folder structure...\n")
print("=" * 80)

success_count = 0
fail_count = 0

for move in moves:
    try:
        # Check if source exists
        if not os.path.exists(move["source"]):
            print(f"⚠️  Skipped: {move['name']}")
            print(f"   Source not found: {move['source']}")
            print()
            continue
            
        # Use mv command to move the notebook
        result = subprocess.run(
            ["mv", move["source"], move["target"]],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"✅ Moved: {move['name']}")
        print(f"   To: {move['folder']}/")
        print()
        success_count += 1
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to move {move['name']}")
        print(f"   Error: {e.stderr if e.stderr else 'Unknown error'}")
        print()
        fail_count += 1
    except Exception as e:
        print(f"❌ Failed to move {move['name']}: {str(e)}")
        print()
        fail_count += 1

print("=" * 80)
print(f"\n🎉 Notebook reorganization complete!")
print(f"   ✅ Successfully moved: {success_count} notebooks")
if fail_count > 0:
    print(f"   ❌ Failed: {fail_count} notebooks")

print(f"\n📁 All notebooks are now organized by function!")
print(f"\n📂 View the new structure at: {base_path}/")

# COMMAND ----------

# DBTITLE 1,Post-Move Cleanup
# MAGIC %md
# MAGIC ## Post-Move Actions
# MAGIC
# MAGIC After running the move operation:
# MAGIC
# MAGIC 1. ✅ **Verify notebooks** are in correct folders
# MAGIC 2. 🗑️ **Delete old notebooks folder** if empty: `RetailPulse/notebooks/`
# MAGIC 3. 📝 **Update any hardcoded paths** in notebooks (if needed)
# MAGIC 4. 🔗 **Update job configurations** to point to new paths
# MAGIC 5. 📚 **Create README** in root folder

# COMMAND ----------

# DBTITLE 1,Verify New Folder Structure
import os

base_path = "/Workspace/Users/shekartelstra@gmail.com/RetailPulse"

print("✅ VERIFICATION: New RetailPulse Folder Structure\n")
print("=" * 80)

folders = [
    "00_Setup",
    "01_DQ_Framework",
    "02_Maintenance",
    "03_Reporting",
    "04_Orchestration",
    "05_Documentation"
]

for folder in folders:
    folder_path = os.path.join(base_path, folder)
    print(f"\n📁 {folder}/")
    
    if os.path.exists(folder_path):
        items = [f for f in os.listdir(folder_path) if f.endswith('.ipynb')]
        if items:
            for item in sorted(items):
                # Remove .ipynb extension for display
                display_name = item.replace('.ipynb', '')
                print(f"   ✅ {display_name}")
        else:
            print("   (empty - ready for future notebooks)")
    else:
        print("   ❌ Folder not found")

print("\n" + "=" * 80)
print("\n🎉 All framework notebooks are now organized by function!")
print("\n📚 Quick Access:")
print("   - Setup notebooks: RetailPulse/00_Setup/")
print("   - DQ Framework: RetailPulse/01_DQ_Framework/")
print("   - Maintenance: RetailPulse/02_Maintenance/")
print("   - Reporting: RetailPulse/03_Reporting/")
print("   - Documentation: RetailPulse/05_Documentation/")
print("\n" + "=" * 80)