__author__ = 'YuWei'
__date__ = '2019-12-26 18:42'

import pandas as pd
import os

def read_tsv(filename):
    if os.path.exists(filename):
        pd.set_option('display.max_columns', None)
        pd_tags = pd.read_table(filepath_or_buffer=filename, sep="\t")
        return pd_tags

def subtracton(y):
    return y

# 获取每小时增量的数据
def process_data(tags):
    # tags = tags.sort_values(by='news_posttime')
    chars = ['-', ' ', ':']
    for char in chars:
        tags['news_posttime'] = tags['news_posttime'].str.replace(char,'')
    tags['news_posttime'] = tags['news_posttime'].str[:10]
    tags_grp = tags.groupby('news_posttime', sort=True)
    hour_incr = tags_grp.news_posttime.agg(len)
    old_list = hour_incr.values
    new_list = []
    for i in range(0, len(old_list)):
        if i == 0:
            new_list.append(0)
        else:
            new_list.append(old_list[i] - old_list[i-1])
    result = pd.DataFrame({"news_posttime": hour_incr.index, "incr": new_list})
    # result = df_hour_incr.rolling(window=2, on='news_posttime', min_periods=1).apply(lambda x: subtracton(x), raw=False)
    return result


def main():
    pd_tags = read_tsv('file/tags.tsv')
    result = process_data(pd_tags)
    result.to_csv("file/incr.tsv", sep=",")


if __name__ == '__main__':
    main()