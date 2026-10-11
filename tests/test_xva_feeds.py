import copy
import json
from pathlib import Path
import sys

import pytest

from risk_pipeline.xva.batch import read_json
from risk_pipeline.xva.feeds import FIELDS, create_input_set, ingest_feed, ingest_submissions, main, validate_ingestion
from risk_pipeline.xva.postgres import transaction
from risk_pipeline.xva.workflow import ingest_inputs, ingest_one_feed
from risk_pipeline.xva.generation import main as generate_main


REPOSITORY = Path(__file__).resolve().parents[1]


@pytest.fixture
def portfolio():
    """Provide a small complete portfolio for independent feed assembly tests."""
    payload = read_json(REPOSITORY / 'examples/xva/inputs.json')
    payload['simulation']['paths'] = 2
    return payload


def stage(portfolio, connection_id, supplied=None):
    """Stage each source independently, optionally reusing explicit versions supplied by a previous run."""
    ids = dict(supplied or {})
    for kind in FIELDS:
        if kind in ids:
            continue
        data = {key: portfolio[key] for key in ('valuation_date','reporting_currency','market','simulation')} if kind == 'settings' else portfolio[kind]
        ids[kind] = ingest_feed(kind, json.dumps(data), f'system:{kind}', portfolio['valuation_date'], connection_id)
    return ids


def assemble(ids, portfolio, connection_id, tmp_path):
    """Bind an explicit set of feed versions to its valuation date and validate the whole portfolio."""
    handoff = create_input_set(ids, portfolio['valuation_date'], connection_id)
    return validate_ingestion({**handoff, 'output_root': str(tmp_path)})


def test_independent_feeds_have_sources_and_explicit_relational_rows(portfolio, pg_connection, tmp_path):
    """Verify separately arriving sources remain independently identified and curve points are flattened."""
    first = ingest_feed('counterparties', json.dumps(portfolio['counterparties']), 'CRM', portfolio['valuation_date'], pg_connection)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute('SELECT feed_type,source,status FROM airflow_xva.input_feed_submissions')
        assert cursor.fetchall() == [('counterparties','CRM','received')]
        cursor.execute('SELECT code,recovery_rate FROM airflow_xva.input_counterparties ORDER BY row_index')
        assert cursor.fetchall() == [('0000001',0.4),('0000002',0.4)]
    ids = stage(portfolio, pg_connection, {'counterparties': first})
    result = assemble(ids, portfolio, pg_connection, tmp_path)
    assert result['accepted_count'] == 1
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute('SELECT COUNT(*) FROM airflow_xva.input_credit_curve_points')
        assert cursor.fetchone()[0] == 4
        cursor.execute("SELECT raw_row->'points' FROM airflow_xva.input_credit_curves")
        assert cursor.fetchall() == [([],),([],)]
        cursor.execute('SELECT COUNT(*) FROM airflow_xva.input_feed_submissions WHERE raw_document IS NOT NULL')
        assert cursor.fetchone()[0] == 0
        cursor.execute("SELECT to_regclass('airflow_xva.input_submissions')")
        assert cursor.fetchone()[0] is None
    assert read_json(result['snapshot_path'])['inputs'] == portfolio


def test_replacing_only_trade_feed_reuses_reference_data(portfolio, pg_connection, tmp_path):
    """Freeze an original set, then bind one new trade version without replacing other sources."""
    ids = stage(portfolio, pg_connection)
    original = assemble(ids, portfolio, pg_connection, tmp_path)
    before = Path(original['snapshot_path']).read_bytes()
    changed = copy.deepcopy(portfolio)
    changed['trades'][0]['notional'] *= 2
    new_trades = ingest_feed('trades', json.dumps(changed['trades']), 'front-office-v2', changed['valuation_date'], pg_connection)
    revised = assemble({**ids,'trades':new_trades}, changed, pg_connection, tmp_path)
    assert revised['batch_id'] != original['batch_id']
    assert Path(original['snapshot_path']).read_bytes() == before
    assert read_json(revised['snapshot_path'])['inputs']['trades'][0]['notional'] == 2_000_000
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute('SELECT COUNT(*) FROM airflow_xva.input_feed_submissions')
        assert cursor.fetchone()[0] == 7


@pytest.mark.parametrize('defect,kind,field', [
    ('missing_counterparty','master_agreements','counterparty_code'),
    ('unknown_curve','counterparties','credit_curve_id'),
    ('unknown_ma','trades','ma_id'),
    ('wrong_counterparty','trades','counterparty_code'),
    ('wrong_csa','trades','csa_id'),
    ('duplicate_csa','csas','ma_id'),
    ('initial_balance','csas','initial_balance'),
    ('not_started','trades','start_date'),
    ('expired','trades','maturity_date'),
    ('curve_date','credit_curves','valuation_date'),
    ('point_order','credit_curves','date'),
    ('survival_increase','credit_curves','survival_probability'),
    ('short_curve','credit_curves','points'),
])
def test_cross_feed_errors_identify_the_source_row(portfolio, pg_connection, tmp_path, defect, kind, field):
    """Reject inconsistent portfolios with an error linked to the responsible feed, row, and field."""
    if defect == 'missing_counterparty':
        portfolio['master_agreements'][0]['counterparty_code'] = '9999999'
    elif defect == 'unknown_curve':
        portfolio['counterparties'][0]['credit_curve_id'] = 'MISSING'
    elif defect == 'unknown_ma':
        portfolio['trades'][0]['ma_id'] = 'MISSING'
    elif defect == 'wrong_counterparty':
        portfolio['trades'][0]['counterparty_code'] = '0000002'
    elif defect == 'wrong_csa':
        portfolio['trades'][0]['csa_id'] = None
    elif defect == 'duplicate_csa':
        portfolio['csas'].append({**portfolio['csas'][0], 'csa_id':'SECOND'})
    elif defect == 'initial_balance':
        portfolio['csas'][0].update(initial_balance=100,receive_collateral=False)
    elif defect == 'not_started':
        portfolio['trades'][0]['start_date'] = '2026-10-11'
    elif defect == 'expired':
        portfolio['trades'][0]['maturity_date'] = '2026-10-10'
    elif defect == 'curve_date':
        portfolio['credit_curves'][0]['valuation_date'] = '2026-10-09'
    elif defect == 'point_order':
        portfolio['credit_curves'][0]['points'][1]['date'] = '2027-10-10'
    elif defect == 'survival_increase':
        portfolio['credit_curves'][0]['points'][1]['survival_probability'] = 0.99
    elif defect == 'short_curve':
        portfolio['credit_curves'][0]['points'].pop()
    ids = stage(portfolio, pg_connection)
    with pytest.raises(ValueError, match='rejected'):
        assemble(ids, portfolio, pg_connection, tmp_path)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute('SELECT feed_id,row_index FROM airflow_xva.input_validation_errors WHERE feed_type=%s AND field=%s', (kind,field))
        errors = cursor.fetchall()
        assert errors and all(feed_id == ids[kind] and index is not None for feed_id,index in errors)
        cursor.execute('SELECT COUNT(*) FROM airflow_xva.input_trades')
        assert cursor.fetchone()[0] == 3  # Bad trades are retained, never silently excluded.


@pytest.mark.parametrize('kind,row,field', [
    ('counterparties',{'code':'BAD','name':'X','recovery_rate':1.2,'credit_curve_id':'X'},'code'),
    ('trades',{'trade_id':'X','notional':'1000'},'notional'),
    ('trades',42,'row'),
    ('settings',{'valuation_date':'not-a-date'},'valuation_date'),
    ('csas',{'post_collateral':'true','receive_collateral':False},'post_collateral'),
    ('csas',{'margin_frequency_days':0,'settlement_lag_days':-1},'margin_frequency_days'),
    ('trades',{'direction':'wrong','currency':'USD','trade_type':'fx_forward'},'direction'),
    ('settings',{'flat_rate':2,'rate_volatility':0.5,'paths':0,'seed':-1},'flat_rate'),
])
def test_invalid_rows_land_before_validation(portfolio, pg_connection, tmp_path, kind, row, field):
    """Stage invalid values as nullable typed columns plus exact raw rows, then persist precise rejection errors."""
    ids = stage(portfolio, pg_connection)
    ids[kind] = ingest_feed(kind, json.dumps([row]), 'bad-source', portfolio['valuation_date'], pg_connection)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute(f'SELECT raw_row FROM airflow_xva.input_{kind} WHERE feed_id=%s', (ids[kind],))
        assert cursor.fetchone()[0] == row
    with pytest.raises(ValueError, match='rejected'):
        assemble(ids, portfolio, pg_connection, tmp_path)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute('SELECT COUNT(*) FROM airflow_xva.input_validation_errors WHERE feed_id=%s AND field=%s', (ids[kind],field))
        assert cursor.fetchone()[0] > 0


def test_missing_mixed_date_and_wrong_type_feeds_are_rejected(portfolio, pg_connection, tmp_path):
    """Block incomplete sets and accidental references to a different source type or valuation date."""
    ids = stage(portfolio, pg_connection)
    missing = dict(ids); missing.pop('trades')
    wrong_type = {**ids,'trades':ids['counterparties']}
    wrong_date = ingest_feed('trades', json.dumps(portfolio['trades']), 'yesterday', '2026-10-09', pg_connection)
    for candidate in (missing, wrong_type, {**ids,'trades':wrong_date}, {**ids,'trades':'does-not-exist'}):
        with pytest.raises(ValueError, match='rejected'):
            assemble(candidate, portfolio, pg_connection, tmp_path)
    # Cross-set defects do not invalidate otherwise sound shared reference data.
    assert assemble(ids, portfolio, pg_connection, tmp_path)['accepted_count'] == 1


def test_duplicate_and_empty_feeds_are_retained_and_rejected(portfolio, pg_connection, tmp_path):
    """Preserve duplicate IDs and explicitly empty required feeds for investigation."""
    ids = stage(portfolio, pg_connection)
    for data in ([], [portfolio['trades'][0],portfolio['trades'][0]]):
        bad = ingest_feed('trades', json.dumps(data), 'bad-trades', portfolio['valuation_date'], pg_connection)
        with pytest.raises(ValueError, match='rejected'):
            assemble({**ids,'trades':bad}, portfolio, pg_connection, tmp_path)
    portfolio['csas'] = []
    for trade in portfolio['trades']:
        trade['csa_id'] = None
    assert assemble(stage(portfolio,pg_connection),portfolio,pg_connection,tmp_path)['accepted_count'] == 1


def test_thousands_of_trades_are_separate_queryable_rows(portfolio, pg_connection, tmp_path):
    """Demonstrate row-based storage and validated freezing for a feed containing thousands of trades."""
    trade = portfolio['trades'][0]
    portfolio['trades'] = [{**trade,'trade_id':f'IRS-{index:05d}'} for index in range(2000)]
    ids = stage(portfolio, pg_connection)
    frozen = assemble(ids, portfolio, pg_connection, tmp_path)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute('SELECT COUNT(*),SUM(notional) FROM airflow_xva.input_trades WHERE feed_id=%s', (ids['trades'],))
        assert cursor.fetchone() == (2000,2_000_000_000)
    assert len(read_json(frozen['snapshot_path'])['inputs']['trades']) == 2000


def test_feed_retries_input_set_retries_and_partial_ingestion_resume(portfolio, pg_connection, tmp_path, monkeypatch):
    """Ensure retries preserve immutable versions and recover a group interrupted after its first alternative."""
    text = json.dumps(portfolio['trades'])
    first = ingest_feed('trades',text,'FO',portfolio['valuation_date'],pg_connection,request_key='feed-retry')
    assert ingest_feed('trades',text,'FO',portfolio['valuation_date'],pg_connection,request_key='feed-retry') == first
    with pytest.raises(ValueError,match='new feed version'):
        ingest_feed('trades',text+' ','FO',portfolio['valuation_date'],pg_connection,request_key='feed-retry')
    ids = stage(portfolio,pg_connection,{'trades':first})
    handoff = create_input_set(ids,portfolio['valuation_date'],pg_connection,request_key='set-retry')
    assert create_input_set(ids,portfolio['valuation_date'],pg_connection,request_key='set-retry') == handoff
    with pytest.raises(ValueError,match='start a new run'):
        create_input_set({},portfolio['valuation_date'],pg_connection,request_key='set-retry')
    import risk_pipeline.xva.feeds as feeds
    actual = feeds.create_input_set
    calls = 0
    def interrupted(*args, **kwargs):
        """Simulate process failure after the first durable input set in a generated group."""
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError('interrupted ingestion')
        return actual(*args, **kwargs)
    texts = [json.dumps(portfolio), json.dumps(portfolio)]
    monkeypatch.setattr(feeds,'create_input_set',interrupted)
    with pytest.raises(RuntimeError,match='interrupted'):
        ingest_submissions(texts,'legacy-adapter',pg_connection,request_key='partial')
    with transaction(pg_connection,readonly=True) as cursor:
        cursor.execute("SELECT ingestion_id FROM airflow_xva.input_ingestions WHERE source='legacy-adapter'")
        partial_id = cursor.fetchone()[0]
    with pytest.raises(ValueError,match='incomplete'):
        validate_ingestion({'ingestion_id':partial_id,'connection_id':pg_connection,'output_root':str(tmp_path)})
    monkeypatch.setattr(feeds,'create_input_set',actual)
    resumed = ingest_submissions(texts,'legacy-adapter',pg_connection,request_key='partial')
    assert validate_ingestion({**resumed,'output_root':str(tmp_path)})['accepted_count'] == 2
    with pytest.raises(ValueError,match='submission count'):
        ingest_submissions(texts[:1],'legacy-adapter',pg_connection,request_key='partial')


def test_accepted_normalized_rows_cannot_change_silently(portfolio, pg_connection, tmp_path):
    """Detect direct database edits to a previously accepted feed set instead of changing frozen history."""
    ids = stage(portfolio,pg_connection)
    frozen = assemble(ids,portfolio,pg_connection,tmp_path)
    before = Path(frozen['snapshot_path']).read_bytes()
    with transaction(pg_connection) as cursor:
        cursor.execute('UPDATE airflow_xva.input_trades SET notional=notional*2 WHERE feed_id=%s', (ids['trades'],))
    with pytest.raises(ValueError,match='rows changed'):
        validate_ingestion({**frozen,'connection_id':pg_connection,'output_root':str(tmp_path)})
    assert Path(frozen['snapshot_path']).read_bytes() == before


def test_explicit_file_feeds_and_feed_id_references(portfolio, pg_connection, tmp_path):
    """Load source-specific files or reuse database feed IDs through the Airflow run configuration."""
    specs = {}
    for kind in FIELDS:
        data = {key:portfolio[key] for key in ('valuation_date','reporting_currency','market','simulation')} if kind == 'settings' else portfolio[kind]
        path = tmp_path/f'{kind}.json'; path.write_text(json.dumps(data))
        specs[kind] = {'path':path.name,'source':f'provider:{kind}'}
    configuration = {'valuation_date':portfolio['valuation_date'],'feeds':specs,'output_root':str(tmp_path/'artifacts')}
    ingested = ingest_inputs(configuration,tmp_path,request_key='file-feeds',connection_id=pg_connection)
    assert validate_ingestion(ingested)['accepted_count'] == 1
    with transaction(pg_connection,readonly=True) as cursor:
        cursor.execute('SELECT feed_type,feed_id FROM airflow_xva.input_set_feeds')
        ids = dict(cursor.fetchall())
    for kind in FIELDS:
        (tmp_path/f'{kind}.json').unlink()
    config = {**configuration,'feeds':{kind:{'feed_id':value} for kind,value in ids.items()}}
    resumed = ingest_inputs(config,tmp_path,connection_id=pg_connection)
    assert validate_ingestion(resumed)['accepted_count'] == 1


def test_default_samples_are_independent_files(tmp_path, pg_connection):
    """Check manual defaults now ingest the six separate sample sources rather than the combined example."""
    handoff = ingest_inputs({'output_root':str(tmp_path)},REPOSITORY,connection_id=pg_connection)
    assert validate_ingestion(handoff)['accepted_count'] == 1
    with transaction(pg_connection,readonly=True) as cursor:
        cursor.execute('SELECT source FROM airflow_xva.input_feed_submissions')
        assert all('/examples/xva/feeds/' in row[0] for row in cursor.fetchall())


def test_legacy_history_is_preserved_when_referenced(portfolio, pg_connection, tmp_path):
    """Migrate an explicitly referenced old JSON ingestion without deleting its original audit record."""
    original = json.dumps(portfolio)
    with transaction(pg_connection) as cursor:
        cursor.execute('''CREATE SCHEMA airflow_xva;
            CREATE TABLE airflow_xva.input_submissions (ingestion_id TEXT,submission_index INTEGER,raw_text TEXT,source TEXT);
            ''')
        cursor.execute("INSERT INTO airflow_xva.input_submissions VALUES ('old-ingestion',0,%s,'old-system')", (original,))
    frozen = validate_ingestion({'ingestion_id':'old-ingestion','connection_id':pg_connection,'output_root':str(tmp_path)})
    assert frozen['accepted_count'] == 1
    with transaction(pg_connection,readonly=True) as cursor:
        cursor.execute('SELECT raw_text FROM airflow_xva.input_submissions')
        assert cursor.fetchone()[0] == original
        cursor.execute('SELECT COUNT(*) FROM airflow_xva.input_trades')
        assert cursor.fetchone()[0] == 3


def test_single_feed_cli_and_invalid_feed_configuration(portfolio,pg_connection,tmp_path,monkeypatch,capsys):
    """Ingest an individual source through the CLI and reject configuration errors without guessing feeds."""
    path = tmp_path/'trades.json'; path.write_text(json.dumps(portfolio['trades']))
    monkeypatch.setattr(sys,'argv',['feeds','--feed-type','trades','--input',str(path),'--valuation-date',portfolio['valuation_date'],
                                  '--connection-id',pg_connection,'--source','trade-system','--request-key','cli-trades'])
    main()
    assert json.loads(capsys.readouterr().out)['feed_type'] == 'trades'
    with pytest.raises(ValueError,match='Unknown feed type'):
        ingest_feed('unknown','[]','source',connection_id=pg_connection)
    with pytest.raises(ValueError,match='Unknown feed types'):
        create_input_set({'unknown':'id'},portfolio['valuation_date'],pg_connection)
    with pytest.raises(ValueError,match='valuation_date'):
        ingest_inputs({'feeds':{}},tmp_path,connection_id=pg_connection)
    with pytest.raises(ValueError,match='Unknown feed type'):
        ingest_inputs({'valuation_date':portfolio['valuation_date'],'feeds':{'unknown':'missing.json'}},tmp_path,connection_id=pg_connection)


def test_independent_airflow_feed_entrypoint_is_idempotent(portfolio,pg_connection,tmp_path):
    """Check the standalone feed DAG helper stages a source without creating an input set or publishing."""
    path = tmp_path/'trades.json'
    path.write_text(json.dumps(portfolio['trades']))
    config = {'feed_type':'trades','input_path':path.name,'valuation_date':portfolio['valuation_date'],'source':'trade-system'}
    first = ingest_one_feed(config,tmp_path,request_key='independent-run',connection_id=pg_connection)
    assert ingest_one_feed(config,tmp_path,request_key='independent-run',connection_id=pg_connection) == first
    with transaction(pg_connection,readonly=True) as cursor:
        cursor.execute('SELECT COUNT(*) FROM airflow_xva.input_sets')
        assert cursor.fetchone()[0] == 0
        cursor.execute('SELECT source FROM airflow_xva.input_feed_submissions')
        assert cursor.fetchone()[0] == 'trade-system'
    for field in ('feed_type','input_path','valuation_date'):
        missing = dict(config); missing.pop(field)
        with pytest.raises(ValueError,match=field):
            ingest_one_feed(missing,tmp_path,connection_id=pg_connection)


def test_generator_writes_separate_feeds_consumable_by_input_dag(tmp_path,pg_connection,monkeypatch):
    """Verify generated independent feed files and their run configuration load without a combined JSON portfolio."""
    root = tmp_path/'generated'
    monkeypatch.setattr(sys,'argv',['generation','--template',str(REPOSITORY/'examples/xva/inputs.json'),
                                  '--feed-directory',str(root),'--count','1','--rejection-rate','0',
                                  '--valuation-date','2026-10-11'])
    generate_main()
    config = read_json(root/'submission-000/run_config.json')
    config['output_root'] = str(tmp_path/'artifacts')
    ingestion = ingest_inputs(config,tmp_path,connection_id=pg_connection)
    assert validate_ingestion(ingestion)['accepted_count'] == 1
    with transaction(pg_connection,readonly=True) as cursor:
        cursor.execute('SELECT COUNT(DISTINCT source) FROM airflow_xva.input_feed_submissions')
        assert cursor.fetchone()[0] == 6


def test_scheduled_default_independent_directory(tmp_path,pg_connection):
    """Read each scheduled source from its own valuation-date directory when no legacy combined feed exists."""
    root = tmp_path/'data/xva/2026-10-10'
    root.mkdir(parents=True)
    for path in (REPOSITORY/'examples/xva/feeds').iterdir():
        (root/path.name).write_bytes(path.read_bytes())
    ingestion = ingest_inputs({'output_root':str(tmp_path/'artifacts')},tmp_path,scheduled_date='2026-10-10',connection_id=pg_connection)
    assert validate_ingestion(ingestion)['accepted_count'] == 1


@pytest.mark.parametrize('points', ['invalid', [42], [{'survival_probability':0.95}]])
def test_invalid_curve_point_rows_remain_auditable(portfolio,pg_connection,tmp_path,points):
    """Retain malformed point containers and point-level defects with curve and point coordinates."""
    portfolio['credit_curves'][0]['points'] = points
    ids = stage(portfolio,pg_connection)
    with pytest.raises(ValueError,match='rejected'):
        assemble(ids,portfolio,pg_connection,tmp_path)
    with transaction(pg_connection,readonly=True) as cursor:
        cursor.execute('SELECT row_index,point_index,field FROM airflow_xva.input_validation_errors WHERE feed_id=%s', (ids['credit_curves'],))
        assert cursor.fetchall()


def test_settings_domains_do_not_override_each_other(portfolio,pg_connection,tmp_path):
    """Keep market values distinct from unrelated simulation or market keys when flattening a settings source."""
    portfolio['simulation']['flat_rate'] = 0.9
    portfolio['market']['reporting_currency'] = 'USD'
    frozen = assemble(stage(portfolio,pg_connection),portfolio,pg_connection,tmp_path)
    inputs = read_json(frozen['snapshot_path'])['inputs']
    assert inputs['market']['flat_rate'] == 0.04
    assert inputs['reporting_currency'] == 'EUR'
