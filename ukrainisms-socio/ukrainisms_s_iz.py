import re
import vk
import pandas as pd
import json
from getpass import getpass
import time
import random
from datetime import date, datetime
import os

COMMUNITIES = {'eurovisionsongcontest': 16785619, 'oldlentach': 29534144, 'f1newsru': 27555325, 'badcomedian': 25557243, 'mudakoff': 57846937, 'dharma_bums': 54566161}


def api_session():
    user_login = input('login: ')
    user_password = getpass('password: ')
    session = vk.AuthSession(app_id=6065285, user_login=user_login, user_password=user_password)
    api = vk.API(session, v='5.65')
    return api


def get_posts(domain):
    posts = []
    posts_100 = SESSION.wall.get(domain=domain, count=100)
    posts += posts_100['items']
    posts_total = posts_100['count']
    while len(posts) < posts_total:
        time.sleep(0.25)
        posts_100 = SESSION.wall.get(domain=domain, count=100, offset=len(posts))
        posts += posts_100['items']
    print('done')
    posts_df = transform_to_pd_df(posts)
    return posts_df


def get_info(df):
    df['city'], df['age'], df['country'], df['sex'] = get_personal_info(df.from_id)
    return df


def get_personal_info_user(i, city, age, country, gender):
	res = SESSION.users.get(user_ids=int(i), fields='city,bdate,country,sex')
    try:
        info = res[0]
    except:
        print(i)
    time.sleep(0.3)
    try:
        if re.search('[0-9]{4}', info['bdate']):
            bd = datetime.strptime(info['bdate'], '%d.%m.%Y')
            age.append(get_age(bd))
        else:
            age.append('None')
    except:
        age.append('None')
    try:
        city.append(info['city']['title'])
    except:
        city.append('None')
    try:
        country.append(info['country']['title'])
    except:
        country.append('None')
    try:
        gender.append(info['sex'])
    except:
        gender.append('None')


def get_personal_info(ids):
    city = []
    age = []
    country = []
    gender = []
    for i in ids:
        get_personal_info_user(i, city, age, country, gender)
    return city, age, country, gender


def get_age(bd):
    today = date.today()
    return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))


def get_comments(owner_id, post_ids, offset):
    comments_df = pd.DataFrame(columns=['text','from_id'])
    for post_id in post_ids[offset:offset + 50]:
        comments_post_df = get_post_comments(owner_id, post_id)
        if type(comments_post_df) != str:
            comments_df = pd.concat([comments_df, comments_post_df])
    return comments_df


def get_post_comments(owner_id, post_id):
    comments = []
    time.sleep(0.3)
    comments_100 = SESSION.wall.getComments(owner_id=-owner_id, post_id=post_id, count=100)
    comments += comments_100['items']
    comments_total = comments_100['count']
    count = 0
    print(comments_total)
    while len(comments) < comments_total:
        time.sleep(0.3)
        comments_100 = SESSION.wall.getComments(owner_id=-owner_id, post_id=post_id, count=100, offset=len(comments))
        comments += comments_100['items']
    try:
        comments_df = transform_to_pd_df(comments)
        return comments_df
    except:
        return ''


def drop_irrelevant_comments(comments, reg):
    rel_comments = pd.DataFrame(columns=list(comments.columns) + ['miss'])
    for i in range(len(comments)):
        res = re.search(reg, str(comments.iloc[i].text), flags=re.I)
        if res:
            rel_comments = rel_comments.append(comments.iloc[i], ignore_index=True)
            rel_comments['miss'].iloc[-1] = res.group(1)
    return rel_comments


def get_data(reg):
    comments_df = None
    for domain in COMMUNITIES:
        posts = get_posts(domain)
        posts.to_csv('posts_%s.csv' % domain)
        # posts = pd.read_csv('posts_%s.csv' % domain)
        print('ok')
        offset = 0
        # while offset < 950:
        while offset < len(posts):
            print(offset)
            comments = get_comments(COMMUNITIES[domain], posts.id, offset)
            backup_time = str(time.localtime().tm_hour) + str(time.localtime().tm_min)
            comments.to_csv('backups/comments_backup_all_%s_%d_%s.csv' % (domain, offset, backup_time))
            comments = drop_irrelevant_comments(comments, reg)
            backup_time = str(time.localtime().tm_hour) + str(time.localtime().tm_min)
            comments.to_csv('backups/comments_backup_%s_%d_%s.csv' % (domain, offset, backup_time))
            print(len(comments))
            offset += 50
            if comments_df is None:
                comments_df = comments
            else:
                comments_df = pd.concat([comments_df, comments])
    # comments_df.to_csv('test_output.csv')
    return get_info(comments_df)


def get_df_from_files(path):
	if '.csv' in path:
		return pd.read_csv(path)
	dir_objs = os.listdir(path)
	df = None
	for obj in dir_objs:
		if '.csv' in obj:
			df_curr = pd.read_csv(path + '/' + obj)
			if df is None:
				df = df_curr
			else:
				df = pd.concat([df, df_curr])
	return df


def process_received_data(path):
	df = get_df_from_files(path)
	return get_info(df)


def process_received_comments(path, reg):
	df = get_df_from_files(path)
	comments = drop_irrelevant_comments(df, reg)
	backup_time = str(time.localtime().tm_hour) + str(time.localtime().tm_min)
	comments.to_csv('comments_%s.csv' % backup_time)
	print('ya sdelyal')
	return get_info(comments)


def save_data():
    places_ = ['России','Украины','Москвы','Питера','Бел[ао]русс?ии','Бел[ао]руси','Кубани','Австрии','Белоруссии','Дании','Албании','Бельгии','Болгарии','Исландии','Андорры','Великобритании','Англии','Шотландии','Венгрии','Латвии','Боснии и Герцеговины','Боснии','Германии','Молдавии','Молдовы','Литвы','Ватикана','Ирландии','Польши','Норвегии','Греции','Лихтенштейна','Финляндии','Испании','Люксембурга','Румынии','Эстонии','Италии','Монако','Словакии','Швеции','Македонии','Нидерландов','Чехии','Франции','Португалии','Швейцарии','Сан[- ]Марино','Сербии','Словении','Хорватии','Черногории','Приднестровья','Казахстана','Турции','Армении','Азербайджана','Грузии','Израиля','Киева','Минска','Севастополя','Уфы','Башкирии','Канады','Австралии', 'екб', 'перми','екатеринбурга','казани','таиланда','китая','японии','америки','вьетнама','индонезии','самары','таджикистана','узбекистана','индии','дуба(?:я|ев|й)','кореи']
    places = '(?:%s(?:$|[^А-Яа-яЁё]))' % '|'.join(places_)
    # reg = '(?:[^А-ЯЁёа-я]|^)(с %s)' % places
    reg = '(?:[^А-ЯЁёа-я]|^)(из %s)' % places
    comments = get_data(reg)
    comments.to_csv('comments_iz.csv')


def transform_to_pd_df(json_file):
    df = pd.DataFrame(json_file)
    tags = ['text', 'from_id', 'id']
    for tag in df:
        if tag not in tags:
            df = df.drop([tag], axis=1)
    df['text'] = debr_text(df['text'])
    return df


def debr_text(texts):
    clean_texts = []
    for text in texts:
        txt = re.sub('<br>','',text)
        clean_texts.append(txt)
    return clean_texts


if __name__ == '__main__':
    global SESSION
    SESSION = api_session()
    save_data()
