'''
Interfaces to be defined:
 - /path/to/executable read --config <config-file-path> --catalog <catalog-file-path> [--state <state-file-path>] > src_message_stream.json
 - cat src_message_stream.json | /path/to/executable generate --config <config-file-path> [--state <state-file-path>] > gen_message_stream.json

'''

import sys
import json
from traceback import format_exc
from importlib import import_module
import click
from dat_core.pydantic_models import ConnectorSpecification, DatCatalog, StreamState

MAX_LEN_ROWS_BUFFER = 999

@click.group()
def cli():
    '''Entry point'''


@cli.command()
@click.option('--config', '-cfg', type=click.File(), required=True)
def discover(config):
    config_mdl = ConnectorSpecification.model_validate_json(config.read())
    SourceClass = getattr(
        import_module(f'verified_sources.{config_mdl.module_name}.source'), config_mdl.name)
    catalog = SourceClass().discover(config=config_mdl)
    click.echo(catalog.model_dump_json())


@cli.command()
@click.option('--config', '-cfg', type=click.File(), required=True)
@click.option('--catalog', '-ctlg', type=click.File(), required=True)
@click.option('--combined-state', '-cmb-state', type=click.File(), required=False)
def read(config, catalog, combined_state):
    from dat_core.pydantic_models import DatMessage, Type, DatLogMessage
    if not combined_state:
        combined_state = {}
    else:
        combined_state = json.loads(combined_state.read())
    _config = config.read()
    base_config_mdl = ConnectorSpecification.model_validate_json(_config)
    ConnectorSpecificationClass = getattr(
        import_module(f'verified_sources.{base_config_mdl.module_name}.specs'), f'{base_config_mdl.name}Specification')
    config_mdl = ConnectorSpecificationClass.model_validate_json(_config)
    SourceClass = getattr(
        import_module(f'verified_sources.{config_mdl.module_name}.source'), config_mdl.name)
    CatalogClass = getattr(
        import_module(f'verified_sources.{config_mdl.module_name}.catalog'), f'{config_mdl.name}Catalog')
    _catalog_read = catalog.read()
    catalog_mdl = CatalogClass.model_validate_json(_catalog_read)
    combined_state_mdl = {_k: StreamState(**_v) for (_k, _v) in combined_state.items()}
    doc_generator = SourceClass().read(
        config=config_mdl, catalog=catalog_mdl, state=combined_state_mdl)
    try:
        for doc in doc_generator:
            click.echo(doc.model_dump_json())
    except Exception as _e:
        click.echo(DatMessage(
            type=Type.LOG,
            log=DatLogMessage(
            level='ERROR',
            message=str(_e),
            stack_trace=format_exc(),
        )).model_dump_json())


@cli.command()
@click.option('--config', '-cfg', type=click.File(), required=True)
def generate(config):
    from dat_core.pydantic_models import DatMessage, Type, DatLogMessage
    # from dat_core.pydantic_models.dat_message import DatDocumentMessage, Data

    config_mdl = ConnectorSpecification.model_validate_json(config.read())
    GeneratorClass = getattr(
        import_module(f'verified_generators.{config_mdl.module_name}.generator'), config_mdl.name)

    for line in sys.stdin:
        try:
            json_line = json.loads(line)
        except json.decoder.JSONDecodeError as _e:
            # click.echo(f'{_e}: {line}', err=True)
            continue
        if json_line['type'] not in ['RECORD',]:
            click.echo(line)
            continue
        e = GeneratorClass().generate(
            config=config_mdl,
            dat_message=DatMessage.model_validate(json_line)
        )
        try:
            for vector in e:
                click.echo(vector.model_dump_json())
        except Exception as _e:
            click.echo(DatMessage(
                type=Type.LOG,
                log=DatLogMessage(
                level='ERROR',
                message=str(_e),
                stack_trace=format_exc(),
            )).model_dump_json())


@cli.command()
@click.option('--config', '-cfg', type=click.File(), required=True)
@click.option('--catalog', '-ctlg', type=click.File(), required=True)
def write(config, catalog):
    from dat_core.pydantic_models import DatMessage, Type, DatLogMessage
    config_mdl = ConnectorSpecification.model_validate_json(config.read())
    DestinationClass = getattr(
        import_module(f'verified_destinations.{config_mdl.module_name}.destination'), config_mdl.name)
    configured_catalog = DatCatalog.model_validate_json(catalog.read())

    rows_buffer = []
    for line in sys.stdin:
        try:
            json_line = json.loads(line)
        except json.decoder.JSONDecodeError as _e:
            # click.echo(f'{_e}: {line}', err=True)
            continue
        if json_line['type'] not in ['RECORD']:
            click.echo(line)
            continue
        rows_buffer.append(
            DatMessage.model_validate(json_line)
        )
        if len(rows_buffer) < MAX_LEN_ROWS_BUFFER:
            continue
        e = DestinationClass().write(
            config=config_mdl,
            configured_catalog=configured_catalog,
            input_messages=rows_buffer,
        )
        for m in e:
            click.echo(m.model_dump_json())
        rows_buffer = []
    if len(rows_buffer):
        e = DestinationClass().write(
            config=config_mdl,
            configured_catalog=configured_catalog,
            input_messages=rows_buffer,
        )
        try:
            for m in e:
                click.echo(m.model_dump_json())
        except Exception as _e:
            click.echo(DatMessage(
                type=Type.LOG,
                log=DatLogMessage(
                level='ERROR',
                message=str(_e),
                stack_trace=format_exc(),
            )).model_dump_json())



if __name__ == '__main__':
    cli()
