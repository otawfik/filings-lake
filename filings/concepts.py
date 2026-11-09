"""Standard line items, and the us-gaap tags that report each one, in priority order.

Companies report the same thing under different tags (revenue alone has a dozen over the years, and
ASC 606 replaced the old ones in 2018). Within one filing, a concept takes the first tag on its list
that the filing reports. Coverage per concept is measured in `python -m filings.coverage`.
"""
from __future__ import annotations

# concept: (period type, tags by priority). "duration" facts cover a period (qtrs 1 or 4), "instant" a date.
CONCEPTS: dict[str, tuple[str, list[str]]] = {
    "revenue": ("duration", [
        "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet", "SalesRevenueGoodsNet", "SalesRevenueServicesNet", "RevenuesNetOfInterestExpense",
        "RegulatedAndUnregulatedOperatingRevenue", "InterestAndDividendIncomeOperating"]),
    "cost_of_revenue": ("duration", [
        "CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold", "CostOfServices", "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization"]),
    "gross_profit": ("duration", ["GrossProfit"]),
    "sga": ("duration", ["SellingGeneralAndAdministrativeExpense", "GeneralAndAdministrativeExpense"]),
    "depreciation": ("duration", [
        "DepreciationDepletionAndAmortization", "DepreciationAndAmortization", "Depreciation", "DepreciationAmortizationAndAccretionNet"]),
    "operating_income": ("duration", ["OperatingIncomeLoss"]),
    "net_income": ("duration", ["NetIncomeLoss", "ProfitLoss", "NetIncomeLossAvailableToCommonStockholdersBasic"]),
    "operating_cash_flow": ("duration", [
        "NetCashProvidedByUsedInOperatingActivities", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]),
    "capex": ("duration", ["PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets"]),
    "eps_diluted": ("duration", ["EarningsPerShareDiluted", "EarningsPerShareBasicAndDiluted"]),
    "cash": ("instant", [
        "CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents", "Cash"]),
    "receivables": ("instant", ["AccountsReceivableNetCurrent", "ReceivablesNetCurrent", "AccountsNotesAndLoansReceivableNetCurrent"]),
    "inventory": ("instant", ["InventoryNet", "InventoryFinishedGoodsNetOfReserves"]),
    "current_assets": ("instant", ["AssetsCurrent"]),
    "securities": ("instant", [
        "MarketableSecuritiesNoncurrent", "AvailableForSaleSecuritiesDebtSecuritiesNoncurrent", "LongTermInvestments"]),
    "ppe": ("instant", [
        "PropertyPlantAndEquipmentNet", "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization"]),
    "total_assets": ("instant", ["Assets"]),
    "current_liabilities": ("instant", ["LiabilitiesCurrent"]),
    "long_term_debt": ("instant", ["LongTermDebtNoncurrent", "LongTermDebt", "LongTermDebtAndCapitalLeaseObligations"]),
    "total_liabilities": ("instant", ["Liabilities"]),
    "equity": ("instant", [
        "StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]),
}

TAGS: set[str] = {t for _, tags in CONCEPTS.values() for t in tags}
PRIORITY: dict[str, tuple[str, int]] = {t: (c, i) for c, (_, tags) in CONCEPTS.items() for i, t in enumerate(tags)}
