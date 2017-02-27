import panda as pd
from lxml import html

for x in range(1, 4009):
    url1 = "http://stats.espncricinfo.com/ci/engine/stats/index.html?class=11;page=" + str(x) + ";template=results;type=batting;view=innings"

    data1 = pd.read_html(url1)

    df1 = data1[2]
    df1.to_csv(path_or_buf='/Users/mohiulalamprince/work/python/cricstat/player-info-%s.csv' % '{:05d}'.format(x), sep=',')
    print str(x) + "  =>  [DONE]"
