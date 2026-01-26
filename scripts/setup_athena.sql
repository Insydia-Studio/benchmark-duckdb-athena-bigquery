-- AWS Athena Setup for Benchmark
-- Run via AWS Athena console or AWS CLI
--
-- First, create database:
-- CREATE DATABASE IF NOT EXISTS ceta_benchmark;

-- stock_eod - EOD stock prices
CREATE EXTERNAL TABLE IF NOT EXISTS ceta_benchmark.stock_eod (
    symbol STRING,
    dateEpoch BIGINT,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adjClose DOUBLE,
    volume BIGINT,
    unadjustedVolume BIGINT,
    change DOUBLE,
    changePercent DOUBLE,
    vwap DOUBLE,
    label STRING,
    changeOverTime DOUBLE
)
STORED AS PARQUET
LOCATION 's3://ceta-benchmark-eu/data_source=fmp/tick_data/eod/';

-- profile - Company profiles
CREATE EXTERNAL TABLE IF NOT EXISTS ceta_benchmark.profile (
    symbol STRING,
    companyName STRING,
    currency STRING,
    cik STRING,
    isin STRING,
    cusip STRING,
    exchange STRING,
    exchangeShortName STRING,
    industry STRING,
    website STRING,
    description STRING,
    ceo STRING,
    sector STRING,
    country STRING,
    fullTimeEmployees STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    zip STRING,
    dcfDiff DOUBLE,
    dcf DOUBLE,
    image STRING,
    ipoDate STRING,
    defaultImage BOOLEAN,
    isEtf BOOLEAN,
    isActivelyTrading BOOLEAN,
    isAdr BOOLEAN,
    isFund BOOLEAN,
    marketCap BIGINT
)
STORED AS PARQUET
LOCATION 's3://ceta-benchmark-eu/data_source=fmp/company/profile/';

-- sp500_constituent - S&P 500 constituents
CREATE EXTERNAL TABLE IF NOT EXISTS ceta_benchmark.sp500_constituent (
    symbol STRING,
    name STRING,
    sector STRING,
    subSector STRING,
    headQuarter STRING,
    dateFirstAdded STRING,
    cik STRING,
    founded STRING
)
STORED AS PARQUET
LOCATION 's3://ceta-benchmark-eu/data_source=fmp/indices/sp500_constituent/';

-- financial_ratios - Financial ratios
CREATE EXTERNAL TABLE IF NOT EXISTS ceta_benchmark.financial_ratios (
    symbol STRING,
    date STRING,
    period STRING,
    currentRatio DOUBLE,
    quickRatio DOUBLE,
    cashRatio DOUBLE,
    daysOfSalesOutstanding DOUBLE,
    daysOfInventoryOutstanding DOUBLE,
    operatingCycle DOUBLE,
    daysOfPayablesOutstanding DOUBLE,
    cashConversionCycle DOUBLE,
    grossProfitMargin DOUBLE,
    operatingProfitMargin DOUBLE,
    pretaxProfitMargin DOUBLE,
    netProfitMargin DOUBLE,
    effectiveTaxRate DOUBLE,
    returnOnAssets DOUBLE,
    returnOnEquity DOUBLE,
    returnOnCapitalEmployed DOUBLE,
    netIncomePerEBT DOUBLE,
    ebtPerEbit DOUBLE,
    ebitPerRevenue DOUBLE,
    debtRatio DOUBLE,
    debtEquityRatio DOUBLE,
    longTermDebtToCapitalization DOUBLE,
    totalDebtToCapitalization DOUBLE,
    interestCoverage DOUBLE,
    cashFlowToDebtRatio DOUBLE,
    companyEquityMultiplier DOUBLE,
    receivablesTurnover DOUBLE,
    payablesTurnover DOUBLE,
    inventoryTurnover DOUBLE,
    fixedAssetTurnover DOUBLE,
    assetTurnover DOUBLE,
    operatingCashFlowPerShare DOUBLE,
    freeCashFlowPerShare DOUBLE,
    cashPerShare DOUBLE,
    payoutRatio DOUBLE,
    operatingCashFlowSalesRatio DOUBLE,
    freeCashFlowOperatingCashFlowRatio DOUBLE,
    cashFlowCoverageRatios DOUBLE,
    shortTermCoverageRatios DOUBLE,
    capitalExpenditureCoverageRatio DOUBLE,
    dividendPaidAndCapexCoverageRatio DOUBLE,
    dividendPayoutRatio DOUBLE,
    priceBookValueRatio DOUBLE,
    priceToBookRatio DOUBLE,
    priceToSalesRatio DOUBLE,
    priceEarningsRatio DOUBLE,
    priceToFreeCashFlowsRatio DOUBLE,
    priceToOperatingCashFlowsRatio DOUBLE,
    priceCashFlowRatio DOUBLE,
    priceEarningsToGrowthRatio DOUBLE,
    priceSalesRatio DOUBLE,
    dividendYield DOUBLE,
    enterpriseValueMultiple DOUBLE,
    priceFairValue DOUBLE
)
STORED AS PARQUET
LOCATION 's3://ceta-benchmark-eu/data_source=fmp/statements/financial_ratios/';

-- analyst_estimates - Analyst estimates
CREATE EXTERNAL TABLE IF NOT EXISTS ceta_benchmark.analyst_estimates (
    symbol STRING,
    date STRING,
    estimatedRevenueLow BIGINT,
    estimatedRevenueHigh BIGINT,
    estimatedRevenueAvg BIGINT,
    estimatedEbitdaLow BIGINT,
    estimatedEbitdaHigh BIGINT,
    estimatedEbitdaAvg BIGINT,
    estimatedEbitLow BIGINT,
    estimatedEbitHigh BIGINT,
    estimatedEbitAvg BIGINT,
    estimatedNetIncomeLow BIGINT,
    estimatedNetIncomeHigh BIGINT,
    estimatedNetIncomeAvg BIGINT,
    estimatedSgaExpenseLow BIGINT,
    estimatedSgaExpenseHigh BIGINT,
    estimatedSgaExpenseAvg BIGINT,
    estimatedEpsAvg DOUBLE,
    estimatedEpsHigh DOUBLE,
    estimatedEpsLow DOUBLE,
    numberAnalystEstimatedRevenue INT,
    numberAnalystsEstimatedEps INT
)
STORED AS PARQUET
LOCATION 's3://ceta-benchmark-eu/data_source=fmp/analyst/analyst_estimates/';

-- Verify tables
SHOW TABLES IN ceta_benchmark;
