import pstats, cProfile
import analyse


if __name__ == '__main__':
    cProfile.runctx("analyse.run()", globals(), locals(), "Profile.prof")
    s = pstats.Stats("Profile.prof")
    s.strip_dirs().sort_stats("time").print_stats()