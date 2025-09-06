#!/usr/bin/env python3
"""
Cashflow analysis utilities (pure pandas + matplotlib).

- Generate sample data (optional)
- Plot & SAVE charts to a user-provided directory (PNG/PDF/SVG or any Matplotlib-supported format)
- No seaborn, one chart per figure, no custom colors

Columns expected in your DataFrame:
    Date (datetime-like or parseable by pandas)
    Flow Type (one of: income, expense, investment — case-insensitive, plural handled)
    Category (string)
    Amount (numeric; ideally income positive, expenses/investments negative)

Quick start (inside Python):
    import pandas as pd
    from cashflow_analysis import (
        generate_sample_finance_df, run_all_charts, SUGGESTED_CATEGORIES
    )

    df = generate_sample_finance_df(start="2025-01-01", months=6)
    run_all_charts(df, save_dir="charts_out", top_n=8, formats=("png","pdf"), dpi=150)

"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime
from dateutil.relativedelta import relativedelta


# ------------------------------
# Suggested category schema
# ------------------------------

SUGGESTED_CATEGORIES = {
    "Income": ["Salary","Bonus","Interest & Dividends","Refunds","Other Income"],
    "Expenses - Fixed": ["Rent/Mortgage","Utilities","Insurance","Phone & Internet","Childcare","Debt Payments","Subscriptions"],
    "Expenses - Variable": ["Groceries","Dining Out","Transportation","Fuel","Public Transit","Health & Fitness","Medical","Entertainment","Shopping","Education","Travel","Gifts/Donations","Home","Pets","Taxes","Fees","Misc"],
    "Investments / Savings": ["401k","IRA","Brokerage","HSA","529","Emergency Fund","High-Yield Savings","Crypto"],
}


# ------------------------------
# Helpers
# ------------------------------

def _normalize_flows(df: pd.DataFrame, flow_col: str = "Flow Type") -> pd.Series:
    s = df[flow_col].astype(str).str.lower().str.strip()
    return s.replace({
        "expenses":"expense",
        "investments":"investment",
        "incomes":"income"
    })


def _ensure_month(df: pd.DataFrame, date_col: str = "Date") -> pd.Series:
    d = pd.to_datetime(df[date_col], errors="coerce")
    return d.dt.to_period("M").dt.to_timestamp()


def _mag(x: float) -> float:
    return float(abs(x))


def _save(fig: plt.Figure, save_dir: str, base: str, formats: Sequence[str] = ("png",), dpi: int = 150) -> List[str]:
    """
    Save a Matplotlib Figure to multiple formats in save_dir.
    Returns the list of saved file paths.
    """
    from pathlib import Path
    outdir = Path(save_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    paths = []
    for ext in formats:
        fpath = outdir / f"{base}.{ext.lstrip('.')}"
        kwargs = {"bbox_inches": "tight"}
        if ext.lower() in ("png", "jpg", "jpeg", "webp", "tif", "tiff"):
            kwargs["dpi"] = dpi
            kwargs["facecolor"] = "white"
        fig.savefig(str(fpath), **kwargs)
        paths.append(str(fpath))
    plt.close(fig)
    return paths


# ------------------------------
# Sample data generator
# ------------------------------

def generate_sample_finance_df(start: str = "2025-01-01", months: int = 8, seed: int = 42) -> pd.DataFrame:
    """
    Create a realistic sample transactions DataFrame with columns:
      - Date (datetime)
      - Flow Type  ('income', 'expense', 'investment')
      - Category
      - Amount     (income > 0, expenses/investments < 0 preferred)
    """
    rng = np.random.default_rng(seed)
    start_dt = pd.to_datetime(start)
    end_dt = (start_dt + relativedelta(months=months)) - pd.Timedelta(days=1)
    dates = pd.date_range(start_dt, end_dt, freq="D")

    income_cats = ["Salary", "Bonus", "Interest & Dividends", "Refunds", "Other Income"]
    expense_fixed = ["Rent/Mortgage", "Utilities", "Insurance", "Phone & Internet", "Subscriptions"]
    expense_var = ["Groceries", "Dining Out", "Transportation", "Fuel", "Health & Fitness",
                   "Medical", "Entertainment", "Shopping", "Education", "Travel",
                   "Gifts/Donations", "Home", "Pets", "Taxes", "Fees", "Misc"]
    invest_cats = ["401k", "IRA", "Brokerage", "HSA", "Emergency Fund", "High-Yield Savings", "529"]

    rows = []

    # Income: 2 paychecks / month + occasional extras
    for month_start in pd.date_range(start_dt, end_dt, freq="MS"):
        for _ in range(2):
            pay_date = month_start + pd.Timedelta(days=int(rng.integers(1, 28)))
            rows.append({"Date": pay_date, "Flow Type": "income", "Category": "Salary",
                         "Amount": float(rng.normal(3500, 200))})
        if rng.random() < 0.25:
            bonus_date = month_start + pd.Timedelta(days=int(rng.integers(1, 28)))
            rows.append({"Date": bonus_date, "Flow Type": "income", "Category": rng.choice(income_cats[1:]),
                         "Amount": float(rng.normal(800, 150))})

    # Fixed expenses: monthly
    for month_start in pd.date_range(start_dt, end_dt, freq="MS"):
        rows.append({"Date": month_start + pd.Timedelta(days=1), "Flow Type": "expense",
                     "Category": "Rent/Mortgage", "Amount": float(-rng.normal(1800, 50))})
        for cat in ["Utilities", "Phone & Internet", "Insurance", "Subscriptions"]:
            if rng.random() < 0.9:
                rows.append({"Date": month_start + pd.Timedelta(days=int(rng.integers(5, 20))),
                             "Flow Type": "expense", "Category": cat,
                             "Amount": float(-rng.normal({"Utilities":180,"Phone & Internet":80,"Insurance":160,"Subscriptions":40}[cat], 15))})

    # Variable expenses: frequent
    for d in dates:
        if d.weekday() in (4, 5, 6) and rng.random() < 0.5:  # groceries
            rows.append({"Date": d, "Flow Type": "expense", "Category": "Groceries",
                         "Amount": float(-rng.normal(70, 20))})
        if rng.random() < 0.25:  # dining
            rows.append({"Date": d, "Flow Type": "expense", "Category": "Dining Out",
                         "Amount": float(-rng.normal(25, 12))})
        for cat, p, mean in [
            ("Transportation", 0.10, 18),
            ("Fuel",           0.08, 45),
            ("Health & Fitness", 0.03, 40),
            ("Entertainment",  0.05, 22),
            ("Shopping",       0.04, 55),
            ("Medical",        0.01, 120),
            ("Education",      0.01, 150),
            ("Travel",         0.005, 220),
            ("Gifts/Donations",0.01, 60),
            ("Home",           0.01, 80),
            ("Pets",           0.01, 35),
            ("Taxes",          0.005, 400),
            ("Fees",           0.02, 8),
            ("Misc",           0.03, 15),
        ]:
            if rng.random() < p:
                rows.append({"Date": d, "Flow Type": "expense", "Category": cat,
                             "Amount": float(-abs(rng.normal(mean, mean*0.4)))})

    # Investments: weekly-ish
    for d in dates[::7]:
        if rng.random() < 0.9:
            rows.append({"Date": d, "Flow Type": "investment", "Category": rng.choice(invest_cats),
                         "Amount": float(-rng.normal(150, 40))})

    df = pd.DataFrame(rows).sort_values("Date").reset_index(drop=True)
    df["Amount"] = df["Amount"].round(2)
    return df


# ------------------------------
# Plotting primitives (each returns saved file paths)
# ------------------------------

def plot_income_vs_expenses_investments(df: pd.DataFrame,
                                        save_dir: str,
                                        base_filename: str = "income_vs_outflows_stacked",
                                        amount_col: str = "Amount",
                                        flow_col: str = "Flow Type",
                                        formats: Sequence[str] = ("png","pdf","svg"),
                                        dpi: int = 150) -> List[str]:
    """One figure: two x positions; second bar stacked: expenses + investments."""
    flow = _normalize_flows(df, flow_col)
    amt = df[amount_col].astype(float)

    income_sum = amt[flow.eq("income")].sum()
    if income_sum <= 0:
        income_sum = df.loc[flow.eq("income"), amount_col].abs().sum()

    exp_signed = amt[flow.eq("expense")].sum()
    inv_signed = amt[flow.eq("investment")].sum()

    exp_mag = -exp_signed if exp_signed < 0 else df.loc[flow.eq("expense"), amount_col].abs().sum()
    inv_mag = -inv_signed if inv_signed < 0 else df.loc[flow.eq("investment"), amount_col].abs().sum()

    fig, ax = plt.subplots()
    x_income, x_out = 0, 1

    ax.bar([x_income], [income_sum], label="Income")
    ax.bar([x_out], [exp_mag], label="Expenses")
    ax.bar([x_out], [inv_mag], bottom=[exp_mag], label="Investments")

    ax.set_xticks([x_income, x_out])
    ax.set_xticklabels(["Income", "Expenses + Investments"])
    ax.set_ylabel("Amount")
    ax.set_title("Income vs (Expenses + Investments) — stacked breakdown")
    ax.legend(loc="best")

    ax.text(x_income, income_sum, f"${income_sum:,.0f}", ha="center", va="bottom")
    if exp_mag > 0:
        ax.text(x_out, exp_mag/2, f"Expenses\n${exp_mag:,.0f}", ha="center", va="center")
    if inv_mag > 0:
        ax.text(x_out, exp_mag + inv_mag/2, f"Investments\n${inv_mag:,.0f}", ha="center", va="center")

    ymax = max(income_sum, exp_mag + inv_mag) * 1.15 if (income_sum and (exp_mag + inv_mag)) else None
    if ymax:
        ax.set_ylim(0, ymax)

    return _save(fig, save_dir, base_filename, formats=formats, dpi=dpi)


def plot_monthly_net_cashflow(df: pd.DataFrame,
                              save_dir: str,
                              base_filename: str = "monthly_net_cashflow",
                              date_col: str = "Date",
                              amount_col: str = "Amount",
                              formats: Sequence[str] = ("png","pdf","svg"),
                              dpi: int = 150) -> List[str]:
    month = _ensure_month(df, date_col)
    monthly = df.assign(_month=month).groupby("_month")[amount_col].sum()

    fig, ax = plt.subplots()
    ax.plot(monthly.index, monthly.values, marker="o")
    ax.axhline(0, linewidth=1)
    ax.set_title("Monthly Net Cash Flow")
    ax.set_xlabel("Month")
    ax.set_ylabel("Net Amount")
    for x, y in zip(monthly.index, monthly.values):
        ax.text(x, y, f"{y:,.0f}", ha="center", va="bottom")
    fig.autofmt_xdate()

    return _save(fig, save_dir, base_filename, formats=formats, dpi=dpi)


def plot_monthly_outflows_by_category(df: pd.DataFrame,
                                      save_dir: str,
                                      base_filename: str = "monthly_outflows_by_category_topN",
                                      date_col: str = "Date",
                                      amount_col: str = "Amount",
                                      flow_col: str = "Flow Type",
                                      category_col: str = "Category",
                                      top_n: int = 8,
                                      formats: Sequence[str] = ("png","pdf","svg"),
                                      dpi: int = 150) -> List[str]:
    flow = _normalize_flows(df, flow_col)
    month = _ensure_month(df, date_col)

    out = df.loc[flow.isin(["expense","investment"])].copy()
    out["_month"] = month.loc[out.index]
    out["_mag"] = out[amount_col].apply(_mag)

    pivot = out.pivot_table(index="_month", columns=category_col, values="_mag", aggfunc="sum").fillna(0)

    totals = pivot.sum().sort_values(ascending=False)
    top_cols = totals.head(top_n).index
    top_pivot = pivot[top_cols].copy()
    if len(totals) > top_n:
        top_pivot["Other"] = pivot.drop(columns=top_cols).sum(axis=1)

    fig, ax = plt.subplots()
    top_pivot.plot(kind="bar", stacked=True, ax=ax)
    ax.set_title(f"Monthly Outflows by Category (Top {top_n})")
    ax.set_xlabel("Month")
    ax.set_ylabel("Amount")
    fig.autofmt_xdate()
    ax.legend(loc="best")

    return _save(fig, save_dir, base_filename, formats=formats, dpi=dpi)


def plot_pareto_outflows(df: pd.DataFrame,
                         save_dir: str,
                         base_filename: str = "pareto_outflows_by_category",
                         amount_col: str = "Amount",
                         flow_col: str = "Flow Type",
                         category_col: str = "Category",
                         formats: Sequence[str] = ("png","pdf","svg"),
                         dpi: int = 150) -> List[str]:
    flow = _normalize_flows(df, flow_col)
    out = df.loc[flow.isin(["expense","investment"])].copy()
    out["_mag"] = out[amount_col].apply(_mag)

    by_cat = out.groupby(category_col)["_mag"].sum().sort_values(ascending=False)
    cum_pct = by_cat.cumsum() / by_cat.sum() * 100.0

    fig, ax1 = plt.subplots()
    ax1.bar(by_cat.index, by_cat.values)
    ax1.set_ylabel("Amount")
    ax1.set_title("Pareto of Outflows by Category")
    ax1.set_xticklabels(by_cat.index, rotation=45, ha="right")

    ax2 = ax1.twinx()
    ax2.plot(by_cat.index, cum_pct.values, marker="o")
    ax2.set_ylabel("Cumulative %")
    ax2.axhline(80, linestyle="--", linewidth=1)

    fig.tight_layout()
    return _save(fig, save_dir, base_filename, formats=formats, dpi=dpi)


def plot_savings_rate(df: pd.DataFrame,
                      save_dir: str,
                      base_filename: str = "savings_rate_by_month",
                      date_col: str = "Date",
                      flow_col: str = "Flow Type",
                      amount_col: str = "Amount",
                      formats: Sequence[str] = ("png","pdf","svg"),
                      dpi: int = 150) -> List[str]:
    flow = _normalize_flows(df, flow_col)
    month = _ensure_month(df, date_col)
    d = df.assign(_month=month, _flow=flow)

    monthly_income = d.loc[d["_flow"].eq("income")].groupby("_month")[amount_col].sum()
    monthly_out = d.loc[d["_flow"].isin(["expense","investment"])].assign(_mag=lambda x: x[amount_col].abs()).groupby("_month")["_mag"].sum()

    sr = (monthly_income - monthly_out) / monthly_income.replace(0, np.nan)

    fig, ax = plt.subplots()
    ax.plot(sr.index, (sr * 100).values, marker="o")
    ax.set_title("Savings Rate by Month")
    ax.set_xlabel("Month")
    ax.set_ylabel("Savings Rate (%)")
    for x, y in zip(sr.index, (sr*100).values):
        if pd.notna(y):
            ax.text(x, y, f"{y:,.1f}%", ha="center", va="bottom")
    fig.autofmt_xdate()

    return _save(fig, save_dir, base_filename, formats=formats, dpi=dpi)



# ------------------------------
# Fixed vs Variable helpers
# ------------------------------

def get_fixed_categories() -> set:
    """Return a set of categories considered 'fixed' by default."""
    return set(SUGGESTED_CATEGORIES.get("Expenses - Fixed", []))

def split_expenses_fixed_variable(df: pd.DataFrame,
                                  flow_col: str = "Flow Type",
                                  category_col: str = "Category",
                                  amount_col: str = "Amount",
                                  fixed_categories: Iterable[str] | None = None) -> tuple[float, float]:
    """
    Return (fixed_total, variable_total) as positive magnitudes.
    Any 'expense' whose Category is NOT in fixed_categories is treated as variable.
    """
    flow = _normalize_flows(df, flow_col)
    fixed_set = set(fixed_categories) if fixed_categories is not None else get_fixed_categories()
    is_expense = flow.eq("expense")
    amounts = df.loc[is_expense, [category_col, amount_col]].copy()
    amounts["_mag"] = amounts[amount_col].abs()
    fixed_total = amounts.loc[amounts[category_col].isin(fixed_set), "_mag"].sum()
    variable_total = amounts.loc[~amounts[category_col].isin(fixed_set), "_mag"].sum()
    return float(fixed_total), float(variable_total)


def plot_income_vs_fixed_variable_investments(df: pd.DataFrame,
                                              save_dir: str,
                                              base_filename: str = "income_vs_fixed_variable_investments",
                                              amount_col: str = "Amount",
                                              flow_col: str = "Flow Type",
                                              category_col: str = "Category",
                                              fixed_categories: Iterable[str] | None = None,
                                              formats: Sequence[str] = ("png","pdf","svg"),
                                              dpi: int = 150) -> List[str]:
    """
    Histogram (bar chart) with four bars:
        Income | Fixed Expenses | Variable Expenses | Investments
    Uses positive magnitudes for outflow bars; Income from signed sum (fallback to |sum| if needed).
    """
    flow = _normalize_flows(df, flow_col)
    amt = df[amount_col].astype(float)

    # Income
    income_sum = amt[flow.eq("income")].sum()
    if income_sum <= 0:
        income_sum = df.loc[flow.eq("income"), amount_col].abs().sum()

    # Outflow components
    fixed_total, variable_total = split_expenses_fixed_variable(df,
                                                                flow_col=flow_col,
                                                                category_col=category_col,
                                                                amount_col=amount_col,
                                                                fixed_categories=fixed_categories)

    invest_signed = amt[flow.eq("investment")].sum()
    invest_total = -invest_signed if invest_signed < 0 else df.loc[flow.eq("investment"), amount_col].abs().sum()

    labels = ["Income", "Fixed Exp.", "Variable Exp.", "Investments"]
    values = [income_sum, fixed_total, variable_total, invest_total]

    fig, ax = plt.subplots()
    ax.bar(labels, values)  # default matplotlib colors
    ax.set_ylabel("Amount")
    ax.set_title("Income vs Fixed/Variable Expenses and Investments")

    for i, v in enumerate(values):
        ax.text(i, v, f"${v:,.0f}", ha="center", va="bottom")

    ax.set_ylim(0, max(values) * 1.15 if any(values) else 1.0)

    return _save(fig, save_dir, base_filename, formats=formats, dpi=dpi)


# ------------------------------
# Orchestrator
# ------------------------------

def run_all_charts(df: pd.DataFrame,
                   save_dir: str,
                   top_n: int = 8,
                   formats: Sequence[str] = ("png","pdf","svg"),
                   dpi: int = 150,
                   include_fixed_variable_chart: bool = True,
                   fixed_categories: Iterable[str] | None = None) -> List[str]:
    """
    Generate and SAVE all charts; returns a flat list of saved file paths.
    """
    paths = []
    if include_fixed_variable_chart:
        paths += plot_income_vs_fixed_variable_investments(df, save_dir, formats=formats, dpi=dpi, fixed_categories=fixed_categories)
    paths += plot_income_vs_expenses_investments(df, save_dir, formats=formats, dpi=dpi)
    paths += plot_monthly_net_cashflow(df, save_dir, formats=formats, dpi=dpi)
    paths += plot_monthly_outflows_by_category(df, save_dir, top_n=top_n, formats=formats, dpi=dpi)
    paths += plot_pareto_outflows(df, save_dir, formats=formats, dpi=dpi)
    paths += plot_savings_rate(df, save_dir, formats=formats, dpi=dpi)
    return paths


# ------------------------------
# CLI demo (optional)
# ------------------------------

if __name__ == "__main__":
    # Minimal demo: generate sample DF and write charts to ./charts_out
    df = generate_sample_finance_df(months=6, seed=7)
    out = run_all_charts(df, save_dir="charts_out", top_n=8, formats=("png","pdf"), dpi=150)
    print("Saved charts:")
    for p in out:
        print(" -", p)
