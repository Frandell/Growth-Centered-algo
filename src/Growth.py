import quantopian.algorithm as algo
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import QTradableStocksUS
from quantopian.pipeline.filters.morningstar import Q1500US
from quantopian.pipeline.data.sentdex import sentiment
from quantopian.pipeline.data.morningstar import operation_ratios
from quantopian.pipeline.data.morningstar import valuation_ratios
from quantopian.pipeline.data.morningstar import earnings_ratios
from quantopian.pipeline.data.morningstar import asset_classification
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import SimpleMovingAverage, VWAP, AverageDollarVolume
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.filters.morningstar import IsPrimaryShare


def initialize(context):
    # Rebalance every day @ open
    algo.schedule_function(
        rebalance,
        algo.date_rules.every_day(),
        algo.time_rules.market_open(hours=0,minutes=1),
        half_days=True
    )

    # Record tracking variables at the end of each day.
    algo.schedule_function(
        record_vars,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(),
    )
    algo.attach_pipeline(make_pipeline(), 'pipeline')


def make_pipeline():
    Alpha_factor = operation_ratios.revenue_growth.latest
    Alpha2 = earnings_ratios.dps_growth.latest
    Alpha3 = asset_classification.value_score.latest
    base_universe = get_tradeable_stocks()
    universe = (Q1500US() & 
                Alpha_factor.notnull() &
                Alpha2.notnull() &
                Alpha3.notnull() 
               & base_universe)
    ##Rankin 
    Alpha_factor =  Alpha_factor.rank(mask = universe, method = 'average')
    Alpha2 = Alpha2.rank(mask = universe, method = 'average')
    Alpha3 = Alpha3.rank(mask = universe, method = 'average')
    Alpha_factor =  Alpha_factor+Alpha2+Alpha3
 
    pipe = Pipeline(columns ={'Alpha_factor': Alpha_factor,
                               'longs': Alpha_factor > 1500,
                                'shorts': Alpha_factor < 500},
                    screen = universe)
    return pipe
  


def before_trading_start(context, data):
    """
    Called every day before market open.
    Not much really happens here
    """
    context.output = algo.pipeline_output('pipeline')
    context.security_list = context.output.index


def rebalance(context, data):
    ##need dynamic weights
    long_secs = context.output[context.output['longs']].index
    short_secs = context.output[context.output['shorts']].index
    print("For the log" , len(long_secs), len(short_secs)) 
    for secuirty in long_secs:
        if data.can_trade(secuirty):
           order_target_percent(secuirty, 0.005)
    for secuirty in short_secs:
        if data.can_trade(secuirty):
            order_target_percent(secuirty, -0.0035)
    for secuirty in context.portfolio.positions:
        if data.can_trade(secuirty) and secuirty not in long_secs and secuirty not in short_secs:
            order_target_percent(secuirty, 0)


def record_vars(context, data):
    ##need better records - Custom 
    long_count = 0
    short_count = 0
    for position in context.portfolio.positions.itervalues():
        if position.amount > 0:
            long_count += 1
        if position.amount < 0:
            short_count += 1         
    # Plot the counts
    record(num_long=long_count, num_short=short_count, leverage=context.account.leverage)
    
"""
Base universe cloned from
https://www.quantopian.com/posts/base-universe-and-masking
"""
def get_tradeable_stocks():
    # Filter for primary share equities. IsPrimaryShare is a built-in filter.
    primary_share = IsPrimaryShare()

    # Equities listed as common stock (as opposed to, say, preferred stock).
    # 'ST00000001' indicates common stock.
    common_stock = morningstar.share_class_reference.security_type.latest.eq('ST00000001')

    # Non-depositary receipts. Recall that the ~ operator inverts filters,
    # turning Trues into Falses and vice versa
    not_depositary = ~morningstar.share_class_reference.is_depositary_receipt.latest

    # Equities not trading over-the-counter.
    not_otc = ~morningstar.share_class_reference.exchange_id.latest.startswith('OTC')

    # Not when-issued equities.
    not_wi = ~morningstar.share_class_reference.symbol.latest.endswith('.WI')

    # Equities without LP in their name, .matches does a match using a regular
    # expression
    not_lp_name = ~morningstar.company_reference.standard_name.latest.matches('.* L[. ]?P.?$')

    # Equities with a null value in the limited_partnership Morningstar
    # fundamental field.
    not_lp_balance_sheet = morningstar.balance_sheet.limited_partnership.latest.isnull()

    # Equities whose most recent Morningstar market cap is not null have
    # fundamental data and therefore are not ETFs.
    have_market_cap = morningstar.valuation.market_cap.latest.notnull()

    # Filter for stocks that pass all of our previous filters.
    tradeable_stocks = (
        primary_share
        & common_stock
        & not_depositary
        & not_otc
        & not_wi
        & not_lp_name
        & not_lp_balance_sheet
        & have_market_cap
    )
    
    return tradeable_stocks
