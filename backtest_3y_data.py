import os

MODE = os.environ.get("RESEARCH_MODE", "PAPER").upper()

if MODE == "PAPER":
    from f2_paper_live import main
elif MODE == "F1":
    from f1_failure_reversal import main
else:
    from f2_regime_research import main

if __name__ == "__main__":
    main()
