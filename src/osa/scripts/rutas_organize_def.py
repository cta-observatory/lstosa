import pathlib
import tarfile
import argparse
import configparser
import datetime

def clean_path(raw_path, base):
    raw_path = raw_path.strip()

    # Sustituir %(BASE)s manualmente
    if "%(BASE)s" in raw_path:
        raw_path = raw_path.replace("%(BASE)s", base)

    # Quedarse desde el primer /
    if "/" in raw_path:
        raw_path = raw_path[raw_path.find("/"):]
    
    return pathlib.Path(raw_path)


# =========================
# LEER CONFIG
# =========================
def load_config(cfg_path):
    cfg_path = pathlib.Path(cfg_path)

    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    config = configparser.ConfigParser(delimiters=(":", "="))
    config.optionxform = str

    config.read(cfg_path)

    if "LST1" not in config:
        print("DEBUG sections:", config.sections())
        raise ValueError("No se encontró la sección [LST1] en el cfg")

    section = config["LST1"]

    # BASE limpio
    base = section.get("BASE").strip()

    # Leer directamente del cfg
    analysis_raw = section.get("ANALYSIS_DIR")
    osa_raw = section.get("OSA_DIR")

    # Limpiar rutas (aquí está la clave)
    running_analysis = clean_path(analysis_raw, base)
    osa_dir = clean_path(osa_raw, base)

    gainsel = osa_dir / "GainSel_log"

    return running_analysis, gainsel

# =========================
# ENCONTRAR v0.11*
# =========================
def find_version_folder(day_path):
    versions = [d for d in day_path.iterdir()
                if d.is_dir() and d.name.startswith("v")]

    if not versions:
        print("❌ No v0.11* directory found")
        return None

    return versions[0]


# =========================
# COMPRIMIR LOGS
# =========================
def compress_logs(base_path, simulate):
    log_path = base_path / "log"

    if not log_path.exists():
        print("[LOG] No log directory")
        return

    err_files = list(log_path.glob("*.err"))
    out_files = list(log_path.glob("*.out"))

    err_tar = log_path / "logs_err.tar.gz"
    out_tar = log_path / "logs_out.tar.gz"

    print(f"[LOG] {len(err_files)} err files")
    print(f"[LOG] {len(out_files)} out files")

    if err_files:
        print(f"[COMPRESS] err → {err_tar.name}")
        if not simulate:
            with tarfile.open(err_tar, "w:gz") as tar:
                for f in err_files:
                    tar.add(f, arcname=f.name)
            for f in err_files:
                f.unlink()

    if out_files:
        print(f"[COMPRESS] out → {out_tar.name}")
        if not simulate:
            with tarfile.open(out_tar, "w:gz") as tar:
                for f in out_files:
                    tar.add(f, arcname=f.name)
            for f in out_files:
                f.unlink()


# =========================
# COMPRIMIR HISTORY
# =========================
def compress_history(base_path, simulate):
    files = list(base_path.glob("*.history"))
    tar_name = base_path / "all_history.tar.gz"

    print(f"[HISTORY] {len(files)} files")

    if not files:
        return

    print(f"[COMPRESS] history → {tar_name.name}")

    if not simulate:
        with tarfile.open(tar_name, "w:gz") as tar:
            for f in files:
                tar.add(f, arcname=f.name)

        for f in files:
            f.unlink()


# =========================
# GAINSEL
# =========================
def compress_gainsel(path, simulate):
    if not path.exists():
        print("[GAINSEL] Path not found")
        return

    check_logs = list(path.glob("*check*.log"))
    normal_logs = [f for f in path.glob("*.log") if "check" not in f.name]

    check_tar = path / "check_logs.tar.gz"
    normal_tar = path / "normal_logs.tar.gz"

    print(f"[GAINSEL] {len(check_logs)} check logs")
    print(f"[GAINSEL] {len(normal_logs)} normal logs")

    if check_logs:
        print(f"[COMPRESS] check → {check_tar.name}")
        if not simulate:
            with tarfile.open(check_tar, "w:gz") as tar:
                for f in check_logs:
                    tar.add(f, arcname=f.name)
            for f in check_logs:
                f.unlink()

    if normal_logs:
        print(f"[COMPRESS] normal → {normal_tar.name}")
        if not simulate:
            with tarfile.open(normal_tar, "w:gz") as tar:
                for f in normal_logs:
                    tar.add(f, arcname=f.name)
            for f in normal_logs:
                f.unlink()


# =========================
# MAIN
# =========================


def main():
    parser = argparse.ArgumentParser(description="Compression tool (sequencer-style)")

    parser.add_argument("-c", "--config", required=True,
                        help="Path to config file")

    parser.add_argument("-d", "--date",
                        help="Date to process (YYYYMMDD), default = yesterday")

    parser.add_argument("-s", "--simulate", action="store_true",
                        help="Simulation mode (no changes)")

    parser.add_argument("--no-gainsel", action="store_true",
                        help="Skip Gain Selection compression")

    parser.add_argument("--no-running", action="store_true",
                        help="Skip Running Analysis compression")

    args = parser.parse_args()

    # Fecha por defecto
    if args.date is None:
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        args.date = yesterday.strftime("%Y%m%d")
        print(f"No date provided → using yesterday: {args.date}")

    print(f"Mode: {'SIMULATION' if args.simulate else 'REAL'}")
    print("=" * 60)

    # Leer config
    running_path, gainsel_path = load_config(args.config)

    # =========================
    # RUNNING ANALYSIS
    # =========================
    if args.no_running:
        print("\n🔹 Running Analysis SKIPPED")
    else:
        day_path = running_path / args.date

        if not day_path.exists():
            print(f"❌ Day not found: {args.date}")
        else:
            print("\n🔹 Running Analysis")
            print(f"Selected: {day_path}")

            version_path = find_version_folder(day_path)

            if version_path:
                print(f"Using version: {version_path.name}")

                compress_logs(version_path, args.simulate)
                compress_history(version_path, args.simulate)

    # =========================
    # GAIN SELECTION
    # =========================
    if args.no_gainsel:
        print("\n🔹 Gain Selection SKIPPED")
    else:
        print("\n🔹 Gain Selection")
        compress_gainsel(gainsel_path, args.simulate)

    print("\nDone.")


if __name__ == "__main__":
    main()
