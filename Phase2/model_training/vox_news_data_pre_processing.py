import pandas as pd
import re
import time

import Constants
import Logger


def read_data(data_path, category_list):
    try:
        logger.info("Cleaning and parsing the head lines...")
        logger.info("Encoding text with ‘utf-8’")
        titles = []
        categories = []
        with open(data_path, 'r', encoding="utf8") as tsv:
            for line in tsv:
                a = line.strip().split('\t')[:3]
                if a[2] in category_list:
                    title = a[0].lower()
                    title = re.sub(r'\s\W', ' ', title)
                    title = re.sub(r'\W\s', ' ', title)
                    titles.append(title)
                    categories.append(a[2])
        return titles, categories
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    start_time = time.time()
    log_file = Logger.LoggerFile()
    logger = log_file.set_logger_file('UCI News Data Pre Processing',
                                      Constants.vox_news_data_pre_processing_log_file_name)

    vox_titles, vox_categories = read_data(Constants.vox_news_data_path, Constants.vox_categories)

    vox_news_data_frame = pd.DataFrame(list(zip(vox_titles, vox_categories)), columns=['Name', 'val'])
    vox_news_data_frame.to_csv(Constants.vox_news_cleaned_data_path, encoding='utf-8')

    logger.info("Data pre processing has completed ")
    logger.info("--- %s seconds ---" % (time.time() - start_time))
