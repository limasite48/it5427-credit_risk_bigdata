"""
Interactive Financial Profile Evaluator & Database Lookup Utility
Supports manual testing for IT5427 Big Data credit risk intelligence pipeline.
"""
import argparse
import json
import sys
import time
import warnings
from pathlib import Path
from typing import Dict, Any, Optional

warnings.filterwarnings("ignore")

# Ensure project root is in sys.path
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.config import TRAIN_DATA_PATH, TEST_DATA_PATH
from src.service.scoring_service import CreditRiskScoringService
from src.data.macro_loader import MacroFeatureJoiner


def lookup_applicant_in_database(sk_id_curr: int) -> Optional[Dict[str, Any]]:
    """
    Search application_train.csv and application_test.csv for an applicant by SK_ID_CURR.
    """
    import pandas as pd

    for file_path, source_name in [(TRAIN_DATA_PATH, "Train Database"), (TEST_DATA_PATH, "Test Database")]:
        if not file_path.exists():
            continue
        # Read in chunks to conserve memory
        for chunk in pd.read_csv(file_path, chunksize=50000):
            matched = chunk[chunk["SK_ID_CURR"] == sk_id_curr]
            if len(matched) > 0:
                row_dict = matched.iloc[0].to_dict()
                row_dict["_database_source"] = source_name
                return row_dict
    return None


def print_profile_card(profile: Dict[str, Any]):
    """Pretty-print the borrower profile card in the terminal."""
    print("\n" + "=" * 70)
    print(f" BORROWER PROFILE SUMMARY [ID: {profile.get('SK_ID_CURR', 'N/A')}]")
    print("=" * 70)

    income = profile.get("AMT_INCOME_TOTAL", 0.0)
    credit = profile.get("AMT_CREDIT", 0.0)
    annuity = profile.get("AMT_ANNUITY", 0.0)
    income_val = float(income) if pd_not_null(income) else 0.0
    credit_val = float(credit) if pd_not_null(credit) else 0.0
    annuity_val = float(annuity) if pd_not_null(annuity) else 0.0

    credit_to_income = (credit_val / income_val) if income_val > 0 else 0.0
    credit_term = (annuity_val / credit_val) if credit_val > 0 else 0.0

    days_birth = profile.get("DAYS_BIRTH")
    age = abs(float(days_birth)) / 365.25 if pd_not_null(days_birth) else "N/A"
    age_str = f"{age:.1f} years" if isinstance(age, (int, float)) else str(age)

    days_employed = profile.get("DAYS_EMPLOYED")
    if pd_not_null(days_employed):
        days_emp_val = float(days_employed)
        emp_str = "Retired / Unemployed" if days_emp_val == 365243 else f"{abs(days_emp_val) / 365.25:.1f} years"
    else:
        emp_str = "N/A"

    print(f" Source Database   : {profile.get('_database_source', 'Custom Input')}")
    print(f" Demographics      : Age {age_str} | Gender: {profile.get('CODE_GENDER', 'N/A')} | Family: {profile.get('NAME_FAMILY_STATUS', 'N/A')}")
    print(f" Employment        : {profile.get('NAME_INCOME_TYPE', 'N/A')} - {profile.get('OCCUPATION_TYPE', 'Unspecified')} ({emp_str})")
    print(f" Education         : {profile.get('NAME_EDUCATION_TYPE', 'N/A')}")
    print(f" Total Income      : {income_val:,.2f}")
    print(f" Requested Credit  : {credit_val:,.2f} (Credit/Income Ratio: {credit_to_income:.2f}x)")
    print(f" Loan Annuity      : {annuity_val:,.2f} (Annuity/Credit Term: {credit_term:.4f})")

    ext1 = profile.get("EXT_SOURCE_1")
    ext2 = profile.get("EXT_SOURCE_2")
    ext3 = profile.get("EXT_SOURCE_3")
    print(f" Bureau Scores     : EXT_1={fmt_val(ext1)} | EXT_2={fmt_val(ext2)} | EXT_3={fmt_val(ext3)}")

    if "TARGET" in profile and pd_not_null(profile["TARGET"]):
        truth = "DEFAULT (TARGET=1)" if int(profile["TARGET"]) == 1 else "REPAID (TARGET=0)"
        print(f" Ground Truth      : {truth}")
    print("-" * 70)


def print_decision_card(decision: Dict[str, Any], ground_truth: Optional[int] = None):
    """Pretty-print the automated credit underwriting decision."""
    pd_pct = decision["default_probability"] * 100
    tier = decision["risk_tier"]
    action = decision["action"]

    # Color/symbol cues
    symbol = "[!]" if tier == "HIGH" else ("[*]" if tier == "MEDIUM" else "[✓]")

    print(f"\n{symbol} CREDIT UNDERWRITING ASSESSMENT & DECISION")
    print(f" Default Probability (PD) : {decision['default_probability']:.4f} ({pd_pct:.2f}%)")
    print(f" Operational Risk Tier    : {tier}")
    print(f" Recommended Action       : {action.upper()}")
    print(f" Operational Description  : {decision.get('tier_description', '')}")

    if ground_truth is not None:
        correct = (int(ground_truth) == 1 and tier == "HIGH") or (int(ground_truth) == 0 and tier != "HIGH")
        status_tag = "MATCHES OUTCOME" if correct else "DISPARATE (Potential FP/FN)"
        print(f" Ground Truth Comparison  : Historical Target={ground_truth} -> [{status_tag}]")

    print("\n Top Decision Drivers (SHAP Value Attribution):")
    drivers = decision.get("top_risk_drivers", [])
    if drivers:
        for idx, d in enumerate(drivers, 1):
            sign = "+" if d["shap_value"] > 0 else ""
            effect_str = "High Risk Indicator" if d["shap_value"] > 0 else "Risk Mitigating Factor"
            print(f"   {idx}. {d['feature']:<25} = {str(d['feature_value']):<10} | SHAP: {sign}{d['shap_value']:.4f} ({effect_str})")
    else:
        print("   (No SHAP attribution returned)")
    print("=" * 70 + "\n")


def pd_not_null(val) -> bool:
    import numpy as np
    return val is not None and not (isinstance(val, float) and np.isnan(val)) and str(val).lower() != "nan"


def fmt_val(val) -> str:
    if pd_not_null(val):
        try:
            return f"{float(val):.3f}"
        except:
            return str(val)
    return "N/A"


def prompt_interactive_profile() -> Dict[str, Any]:
    """Interactively prompt user for financial profile attributes in the console."""
    print("\n" + "=" * 70)
    print(" INTERACTIVE APPLICANT FINANCIAL PROFILE CREATOR")
    print(" Press ENTER to accept the default recommended values.")
    print("=" * 70)

    def get_input(prompt_text, default_val, val_type=float):
        user_val = input(f"{prompt_text} [{default_val}]: ").strip()
        if not user_val:
            return default_val
        try:
            return val_type(user_val)
        except:
            return default_val

    income = get_input("1. Total Annual Income (AMT_INCOME_TOTAL)", 220000.0, float)
    credit = get_input("2. Requested Loan Credit (AMT_CREDIT)", 450000.0, float)
    annuity = get_input("3. Annual Loan Annuity (AMT_ANNUITY)", 22500.0, float)
    age = get_input("4. Applicant Age in Years", 36.0, float)
    employed_yrs = get_input("5. Years Employed", 6.0, float)

    print("\nIncome Type Options: Working, Commercial associate, State servant, Pensioner")
    income_type = input("6. Income Type [Working]: ").strip() or "Working"

    print("Occupation Options: Laborers, Drivers, Sales staff, Managers, Accountants, High skill tech staff")
    occupation = input("7. Occupation [Laborers]: ").strip() or "Laborers"

    ext1 = get_input("8. Bureau Score 1 (EXT_SOURCE_1, 0.0-1.0)", 0.55, float)
    ext2 = get_input("9. Bureau Score 2 (EXT_SOURCE_2, 0.0-1.0)", 0.60, float)
    ext3 = get_input("10. Bureau Score 3 (EXT_SOURCE_3, 0.0-1.0)", 0.50, float)

    profile = {
        "SK_ID_CURR": 999001,
        "AMT_INCOME_TOTAL": income,
        "AMT_CREDIT": credit,
        "AMT_ANNUITY": annuity,
        "DAYS_BIRTH": -int(age * 365.25),
        "DAYS_EMPLOYED": -int(employed_yrs * 365.25),
        "NAME_INCOME_TYPE": income_type,
        "OCCUPATION_TYPE": occupation,
        "NAME_EDUCATION_TYPE": "Secondary / secondary special",
        "NAME_FAMILY_STATUS": "Married",
        "CODE_GENDER": "M",
        "EXT_SOURCE_1": ext1,
        "EXT_SOURCE_2": ext2,
        "EXT_SOURCE_3": ext3,
        "_database_source": "Interactive Console Input",
    }
    return profile


def main():
    parser = argparse.ArgumentParser(
        description="Credit Risk Manual Testing & Evaluation Utility (IT5427 Big Data)"
    )
    parser.add_argument(
        "--lookup-id",
        type=int,
        metavar="ID",
        help="Search database (application_train/test) by SK_ID_CURR and evaluate profile",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Interactive terminal prompt to input a new financial profile",
    )
    parser.add_argument(
        "--input-json",
        type=str,
        metavar="JSON_OR_PATH",
        help="Pass a JSON string or path to a JSON file containing applicant financial features",
    )
    parser.add_argument(
        "--include-macro",
        action="store_true",
        help="Join supplementary regional macroeconomic indicators point-in-time",
    )
    args = parser.parse_args()

    service = CreditRiskScoringService()

    # Mode 1: Lookup existing record from database
    if args.lookup_id:
        print(f"Searching database for applicant SK_ID_CURR = {args.lookup_id}...")
        t0 = time.perf_counter()
        profile = lookup_applicant_in_database(args.lookup_id)
        search_ms = (time.perf_counter() - t0) * 1000

        if not profile:
            print(f"Error: Applicant ID {args.lookup_id} not found in application_train.csv or application_test.csv.")
            sys.exit(1)

        print(f"Found applicant in {search_ms:.1f}ms.")
        if args.include_macro:
            import pandas as pd
            try:
                joiner = MacroFeatureJoiner()
                enriched = joiner.join_macro_features(pd.DataFrame([profile]))
                profile = enriched.iloc[0].to_dict()
            except Exception as e:
                print(f"Macro warning: {e}")

        print_profile_card(profile)
        decision = service.score_applicant(profile, include_explanations=True)
        ground_truth = profile.get("TARGET")
        print_decision_card(decision, ground_truth=ground_truth)

    # Mode 2: Interactive terminal prompt
    elif args.interactive:
        profile = prompt_interactive_profile()
        print_profile_card(profile)
        decision = service.score_applicant(profile, include_explanations=True)
        print_decision_card(decision)

    # Mode 3: Input JSON string or file
    elif args.input_json:
        json_path = Path(args.input_json)
        if json_path.exists() and json_path.is_file():
            profile = json.loads(json_path.read_text(encoding="utf-8"))
        else:
            try:
                profile = json.loads(args.input_json)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                sys.exit(1)

        profile.setdefault("_database_source", "Custom JSON Input")
        print_profile_card(profile)
        decision = service.score_applicant(profile, include_explanations=True)
        print_decision_card(decision)

    else:
        # Default fallback demonstration
        print("No execution mode selected. Showing demo lookup for applicant 100002 (Train Database):")
        profile = lookup_applicant_in_database(100002)
        if profile:
            print_profile_card(profile)
            decision = service.score_applicant(profile, include_explanations=True)
            print_decision_card(decision, ground_truth=profile.get("TARGET"))
        print("\nTip: Run with --help to see manual testing options:\n"
              "  python evaluate_profile.py --lookup-id 100002\n"
              "  python evaluate_profile.py --interactive\n"
              "  python evaluate_profile.py --input-json '{\"AMT_INCOME_TOTAL\": 300000, \"AMT_CREDIT\": 400000}'")


if __name__ == "__main__":
    main()
