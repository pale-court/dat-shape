import cProfile, pstats

profiler = cProfile.Profile()

profiler.enable()
import dat_shape.console
dat_shape.console.run()
profiler.disable()

stats = pstats.Stats(profiler).sort_stats('tottime')
stats.dump_stats('dat_shape.prof')