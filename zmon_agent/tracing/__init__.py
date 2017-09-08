import opentracing


def init_opentracing_tracer(tracer, debug_level):
    if tracer == 'instana':
        import instana.options as instanaOpts  # noqa
        import instana.tracer  # noqa

        instana.tracer.init(instanaOpts.Options(service='zmon-agent', log_level=debug_level))
    elif tracer == 'basic':
        from basictracer import BasicTracer  # noqa

        opentracing.tracer = BasicTracer()

    else:
        opentracing.tracer = opentracing.Tracer()
