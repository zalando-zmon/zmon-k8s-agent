import opentracing

from enum import Enum


class Tracers(Enum):
    BASIC = 'basic'
    INSTANA = 'instana'


def init_opentracing_tracer(tracer, debug_level):
    if tracer == Tracers.INSTANA:
        import instana.options as instanaOpts  # noqa
        import instana.tracer  # noqa

        instana.tracer.init(instanaOpts.Options(service='zmon-agent', log_level=debug_level))
    elif tracer == Tracers.BASIC:
        from basictracer import BasicTracer  # noqa

        opentracing.tracer = BasicTracer()

    else:
        opentracing.tracer = opentracing.Tracer()
