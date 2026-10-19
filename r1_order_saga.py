"""R1 order execution saga with strict fail-closed boundaries.

NO live exchange adapter is supplied. An injected adapter must implement the
interface and independently prove live order + conditional-stop semantics.
This module is simulation-only until that verification is completed.
"""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol


def D(value):
    x = Decimal(str(value))
    if not x.is_finite():
        raise ValueError('non-finite number')
    return x


class Broker(Protocol):
    def submit_entry(self, client_id: str, symbol: str, side: str, quantity: Decimal) -> str: ...
    def find_entry(self, client_id: str) -> dict: ...
    def submit_stop(self, client_id: str, symbol: str, side: str, quantity: Decimal, trigger: Decimal) -> str: ...
    def find_stop(self, client_id: str) -> dict: ...
    def emergency_flatten(self, symbol: str, side: str, quantity: Decimal) -> str: ...
    def find_position(self, symbol: str, side: str) -> dict: ...


@dataclass
class Result:
    state: str
    entry_id: str = ''
    stop_id: str = ''
    events: list = field(default_factory=list)
    permit_new_entries: bool = False


class SafetyHalt(Exception):
    pass


class OrderSaga:
    """A single order attempt. NOT crash-safe without persistent state and replay.

    All errors after entry submission are uncertain; emergency flatten is tried,
    and fresh entry authorization is NEVER returned. Operator review required.
    """
    def __init__(self, broker: Broker, *, verified_live_adapter=False):
        self.broker = broker
        self.verified_live_adapter = verified_live_adapter

    def execute(self, *, client_id, symbol, side, quantity, stop_trigger):
        if not self.verified_live_adapter:
            raise SafetyHalt('No verified live adapter: execution prohibited')
        if not client_id or not symbol or side not in ('LONG', 'SHORT'):
            raise ValueError('invalid order identity')
        qty, trigger = D(quantity), D(stop_trigger)
        if qty <= 0 or trigger <= 0:
            raise ValueError('invalid quantity or trigger')
        result = Result(state='PREPARED')
        try:
            result.state = 'ENTRY_UNCERTAIN'
            result.entry_id = str(self.broker.submit_entry(client_id, symbol, side, qty))
            entry = self.broker.find_entry(client_id)
            if (entry.get('status') != 'FILLED' or
                D(entry.get('filled_qty', 0)) != qty or
                str(entry.get('order_id')) != result.entry_id):
                raise SafetyHalt('Entry not fully verified')
            result.events.append('ENTRY_FILLED')
            result.state = 'STOP_UNCERTAIN'
            stop_client_id = client_id + '-SL'
            result.stop_id = str(self.broker.submit_stop(stop_client_id, symbol, side, qty, trigger))
            stop = self.broker.find_stop(stop_client_id)
            if (str(stop.get('order_id')) != result.stop_id or
                stop.get('status') != 'ACTIVE' or
                stop.get('reduce_only') is not True or
                stop.get('side') != side or
                stop.get('symbol') != symbol or
                D(stop.get('quantity', 0)) < qty or
                D(stop.get('trigger', 0)) != trigger):
                raise SafetyHalt('Exchange-side stop unverified')
            result.events.append('STOP_VERIFIED')
            result.state = 'PROTECTED_PENDING_RECONCILIATION'
            return result
        except Exception as exc:
            result.events.append('HALT:' + type(exc).__name__)
            result.state = 'EMERGENCY_FLATTEN_UNCERTAIN'
            try:
                result.events.append('FLATTEN_REQUEST:' + str(self.broker.emergency_flatten(symbol, side, qty)))
                pos = self.broker.find_position(symbol, side)
                if D(pos.get('quantity', -1)) == 0:
                    result.state = 'FLAT_CONFIRMED_REVIEW_REQUIRED'
            except Exception:
                result.events.append('FLATTEN_OR_CONFIRMATION_FAILED')
            return result


class FakeBroker:
    def __init__(self, *, fail_stop=False, fail_fill=False, fail_flat=False):
        self.fail_stop, self.fail_fill, self.fail_flat = fail_stop, fail_fill, fail_flat
        self.qty = Decimal(0)
        self.order_id = ''
        self.stop_id = ''
    def submit_entry(self, client_id, symbol, side, quantity):
        self.qty = quantity
        self.order_id = 'entry-test'
        return self.order_id
    def find_entry(self, client_id):
        return {'status':'PARTIALLY_FILLED' if self.fail_fill else 'FILLED',
                'filled_qty':str(self.qty),'order_id':self.order_id}
    def submit_stop(self, client_id, symbol, side, quantity, trigger):
        if self.fail_stop: raise RuntimeError('simulated stop rejection')
        self.stop_id = 'stop-test'
        self.trigger = trigger
        self.side, self.symbol = side, symbol
        return self.stop_id
    def find_stop(self, client_id):
        return {'order_id':self.stop_id,'status':'ACTIVE','reduce_only':True,
                'side':self.side,'symbol':self.symbol,'quantity':str(self.qty),'trigger':str(self.trigger)}
    def emergency_flatten(self, symbol, side, quantity):
        if self.fail_flat: raise RuntimeError('simulated flatten failure')
        self.qty = Decimal(0)
        return 'flatten-test'
    def find_position(self, symbol, side):
        return {'quantity':str(self.qty)}


def self_test():
    kwargs=dict(client_id='test-123',symbol='BTC-USDT',side='LONG',quantity='0.01',stop_trigger='97500')
    count=0
    try: OrderSaga(FakeBroker()).execute(**kwargs)
    except SafetyHalt: count+=1
    else: raise AssertionError('unverified adapter accepted')
    r=OrderSaga(FakeBroker(),verified_live_adapter=True).execute(**kwargs)
    assert r.state=='PROTECTED_PENDING_RECONCILIATION' and not r.permit_new_entries
    count+=1
    for opts,expected in [({'fail_stop':True},'FLAT_CONFIRMED_REVIEW_REQUIRED'),
                          ({'fail_fill':True},'FLAT_CONFIRMED_REVIEW_REQUIRED'),
                          ({'fail_stop':True,'fail_flat':True},'EMERGENCY_FLATTEN_UNCERTAIN')]:
        r=OrderSaga(FakeBroker(**opts),verified_live_adapter=True).execute(**kwargs)
        assert r.state==expected and not r.permit_new_entries
        count+=1
    for bad in ('NaN','-1','0'):
        try: OrderSaga(FakeBroker(),verified_live_adapter=True).execute(**{**kwargs,'quantity':bad})
        except ValueError: count+=1
        else: raise AssertionError('invalid quantity accepted')
    print(f'PASS: {count} mock execution/stop/emergency cases')
    print('NOT LIVE READY: no verified BingX order adapter or durable crash recovery')

if __name__=='__main__':
    self_test()
