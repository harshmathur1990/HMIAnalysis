tmux new-session -d -s session_2011 '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2011 1 1 365 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2011'
tmux new-session -d -s session_2012_start '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2012 1 1 157 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2012_start'
tmux new-session -d -s session_2012_mid '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2012 6 7 119 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2012_mid'
tmux new-session -d -s session_2012_end '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2012 10 5 88 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2012_end'
tmux new-session -d -s session_2013 '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2013 1 1 365 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2013'
tmux new-session -d -s session_2014_start '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2014 1 1 296 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2014_start'
tmux new-session -d -s session_2014_end '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2014 10 25 68 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2014_end'
tmux new-session -d -s session_2015_start '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2015 1 1 360 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2015_start'
tmux new-session -d -s session_2015_end '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2015 12 28 4 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2015_end'
tmux new-session -d -s session_2016_start '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2016 1 1 215 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2016_start'
tmux new-session -d -s session_2016_end '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2016 8 4 150 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2016_end'
tmux new-session -d -s session_2017_start '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2017 1 1 150 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2017_start'
tmux new-session -d -s session_2017_end '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2017 6 1 213 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2017_end'
tmux new-session -d -s session_2018_start '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2018 1 1 285 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2018_start'
tmux new-session -d -s session_2018_end '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2018 10 14 79 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2018_end'
tmux new-session -d -s session_2019 '/data/harsh1/HMIAnalysis/env/bin/python -u analyse.py 2019 1 1 190 1 2>&1 | tee /data/harsh1/HMIAnalysis/outfile_2019'