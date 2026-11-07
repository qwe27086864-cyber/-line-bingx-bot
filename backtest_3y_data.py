import os

MODE = os.environ.get("RESEARCH_MODE", "F2").upper()

if MODE == "F1":
    from f1_failure_reversal import main
else:
    from f2_regime_research import main

if __name__ == "__main__":
    main()
