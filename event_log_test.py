import pandas as pd
#import numpy as np


file_in = r'C:\Users\zombi\Desktop\Новая\event_log_test_v001.xlsx'

#df_cases = pd.read_excel(file_in, sheet_name = "cases", header = 0, index_col = 0)

df_events = pd.read_excel(file_in, sheet_name = "events", header = 0, index_col = 0)
df_event_log_target = pd.read_excel(file_in, sheet_name = "event_log", header = 0, index_col = 0)

#type(df_events.dt.iloc[0])
#df_events['dt_add'] = pd.to_datetime(df_events['dt'], format = '%Y-%m-%d %H:%M:%S')

events_unique = df_event_log_target['activity'].unique()

df_logg = df_events.copy()
# =============================================================================
# Поиск смены исполнителя
# =============================================================================
def check_change_executor(content):
    res = ''
    m_cont = content.strip().lower().replace('ё','е') 
    if (m_cont.find('смена исполнителя') != -1) or (m_cont.find('вы назначены исполнителем') != -1):
        res = 'Смена исполнителя'      
    return res

df_logg['executor_change'] = list(map(lambda content: check_change_executor(str(content)), df_logg['content']))

# ==================================
# Поиск комментария
# ==================================
def check_comment(content):
    res = ''
    m_cont = content.strip().lower().replace('ё','е') 
    if (m_cont.find('комментарий') != -1):
        res = 'Добавление комментария'      
    return res

df_logg['comment'] = list(map(lambda content: check_comment(str(content)), df_logg['action']))
# ==================================
# Поиск вложений
# ==================================
def check_file(content):
    res = ''
    m_cont = content.strip().lower().replace('ё','е') 
    if (m_cont.find('файл') != -1):
        res = 'Загрузка файла'      
    return res

df_logg['file'] = list(map(lambda content: check_file(str(content)), df_logg['action']))

# =============================================================================
# Поиск контроль выполнения работ
# =============================================================================
def check_control(content):
    res = ''
    # del spaces before text and after it, transfer to low register, change specific letters
    m_cont = content.strip().lower().replace('ё','е') 
    if (m_cont.find('задание: контроль выполнения работ') != -1) or (m_cont.find('звонок контроля выполнения работ ') != -1):
        res = 'Контроль выполнения'      
    return res

df_logg['control'] = list(map(lambda content: check_control(str(content)), df_logg['action']))

# =============================================================================
# Поиск смены ответственного
# =============================================================================
def check_change_controller(content):
    res = ''
    m_cont = content.strip().lower().replace('ё','е') 
    if (m_cont.find('ответственный сменился') != -1) or (m_cont.find('вы назначены ответственным') != -1):
        res = 'Смена ответственного'   
    return res

df_logg['controller_change'] = list(map(lambda content: check_change_controller(str(content)), df_logg['content']))

# =============================================================================
# Поиск смены статуса 
# =============================================================================
# ищем строку "статус сменился с принята в работу на выполнена"
def check_action_status(content):
    res = '' 
    m_cont = content.strip().lower().replace('ё','е') 
    m_cont_split = m_cont.split(';')
    # ищем смену статуса
    for sse in m_cont_split: # sse = ssl[0]
        sse = sse.strip()
        rf = sse.find('статус сменился')
        if ((rf != -1) and (res == '')): 
            sse = sse[rf:] # убираем мусор в начале
            # проверяем повторы
            sse2 = sse[len('статус сменился'):] # отступим на ключевую фразу и найдем ее еще раз
            rf2 = sse2.find('статус сменился')
            if rf2 !=- 1: sse = 'статус сменился' + sse2[:rf2]
            ###
            res = res + sse 
            res = res.replace('<br>','')
            res = res.replace('<br />','')
            break        
    return res
# делаем колонку
df_logg['action_status'] = list(map(lambda content: check_action_status(str(content)), df_logg['content']))
# =============================================================================
# выбираем статус "с" 
# action_status = 'Статус сменился с Принята в работу на Требуется передать'
def get_status_from(action_status):
    res = ''
    m_action_status = action_status.strip().lower().replace('ё','е')    
    if len(m_action_status)>0:
        ssl = m_action_status.split(' ') 
        if ("с" in ssl) and ("на" in ssl):
            res = ' '.join(ssl[ssl.index("с")+1: ssl.index("на")])
            res = '<' + res.strip() + '>'
    return res
# делаем колонку
df_logg['from_status'] = list(map(lambda ss: get_status_from(str(ss)), df_logg['action_status']))

# =============================================================================
# выбираем статус "на" 
# action_status = 'Статус сменился с Принята в работу на Требуется передать'
def get_status_to(action_status):
    res = ''
    m_action_status = action_status.strip().lower().replace('ё','е')    
    if len(m_action_status)>0:
        ssl = m_action_status.split(' ') # split content string to list via ' '
        if ("с" in ssl) and("на" in ssl):
            res = ' '.join(ssl[ssl.index("на")+1: ])
            res = '<' + res.strip() + '>'
    return res
# делаем колонку
df_logg['to_status'] = list(map(lambda ss: get_status_to(str(ss)), df_logg['action_status']))
# регистрация заявки 
def check_reg(sdf):
    res = ''
    l_index_unique = list(df_logg.index.unique())
    l1, l2 = [], []
    rel = sdf
    sdf, dt_min, dt_min_u = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # создадим серию с case_id и timestamp
    for i in l_index_unique:
        sdf = pd.concat([sdf, df_logg.loc[i]]) 
        dt_min = sdf.iloc[:,0]
        
    # получим минимальную дату для каждого case_id 
    for i in l_index_unique: 
        l1.append(dt_min.loc[i:])
    dt_min_u = dt_min.groupby(level = 0).transform('min')
    for i in l_index_unique:
        l2.append(str(dt_min_u.loc[i].iloc[0]))    
    
    # сопоставим полученные даты с датами в исходном датафрейме
    for i in l2:
        if str(rel).find(i) != -1:
            res = '<зарегистрирована>'
    return res 

df_logg['registration'] = list(map(lambda ss: check_reg(ss), map(str, list(df_logg['dt']))))
# =============================================================================
# Выбираем события
# =============================================================================
tmp1_log = df_logg[df_logg['registration']!='']
counter = 0

tmp1_df = pd.DataFrame()
tmp1_df['case_id'] = tmp1_log.index
tmp1_df['event_id'] = [i for i in range(counter, counter + len(tmp1_df))]
tmp1_df['timestamp'] = list(tmp1_log['dt'])
tmp1_df['subject'] = list(tmp1_log['subject'])
tmp1_df['activity'] = list(tmp1_log['registration'])
counter = counter + len(tmp1_df)

tmp2_log = df_logg[df_logg['to_status']!='']

tmp2_df = pd.DataFrame()
tmp2_df['case_id'] = tmp2_log.index
tmp2_df['event_id'] = [i for i in range(counter, counter + len(tmp2_df))]
tmp2_df['timestamp'] = list(tmp2_log['dt'])
tmp2_df['subject'] = list(tmp2_log['subject'])
tmp2_df['activity'] = list(tmp2_log['to_status'])
counter = counter + len(tmp2_df)

# добавление комментария
tmp3_log = df_logg[df_logg['comment']!='']

tmp3_df = pd.DataFrame()
tmp3_df['case_id'] = tmp3_log.index
tmp3_df['event_id'] = [i for i in range(counter, counter + len(tmp3_df))]
tmp3_df['timestamp'] = list(tmp3_log['dt'])
tmp3_df['subject'] = list(tmp3_log['subject'])
tmp3_df['activity'] = list(tmp3_log['comment'])
counter = counter + len(tmp3_df)

# добавление метки контроль выполнения работ
tmp4_log = df_logg[df_logg['control']!='']

tmp4_df = pd.DataFrame()
tmp4_df['case_id'] = tmp4_log.index
tmp4_df['event_id'] = [i for i in range(counter, counter + len(tmp4_df))]
tmp4_df['timestamp'] = list(tmp4_log['dt'])
tmp4_df['subject'] = list(tmp4_log['subject'])
tmp4_df['activity'] = list(tmp4_log['control'])
counter = counter + len(tmp4_df)

# добавление файла
tmp5_log = df_logg[df_logg['file']!='']

tmp5_df = pd.DataFrame()
tmp5_df['case_id'] = tmp5_log.index
tmp5_df['event_id'] = [i for i in range(counter, counter + len(tmp5_df))]
tmp5_df['timestamp'] = list(tmp5_log['dt'])
tmp5_df['subject'] = list(tmp5_log['subject'])
tmp5_df['activity'] = list(tmp5_log['file'])
counter = counter + len(tmp5_df)

tmp6_log = df_logg[df_logg['controller_change']!='']

tmp6_df = pd.DataFrame()
tmp6_df['case_id'] = tmp6_log.index
tmp6_df['event_id'] = [i for i in range(counter, counter + len(tmp6_df))]
tmp6_df['timestamp'] = list(tmp6_log['dt'])
tmp6_df['subject'] = list(tmp6_log['subject'])
tmp6_df['activity'] = list(tmp6_log['controller_change'])
counter = counter + len(tmp6_df)

# смена исполнителя
tmp7_log = df_logg[df_logg['executor_change']!='']

tmp7_df = pd.DataFrame()
tmp7_df['case_id'] = tmp7_log.index
tmp7_df['event_id'] = [i for i in range(counter, counter + len(tmp7_df))]
tmp7_df['timestamp'] = list(tmp7_log['dt'])
tmp7_df['subject'] = list(tmp7_log['subject'])
tmp7_df['activity'] = list(tmp7_log['executor_change'])
counter = counter + len(tmp7_df)

# =============================================================================
# собираем Event Log
# =============================================================================
df_res_log = tmp1_df.copy()                
# df_res_log = df_res_log.append(tmp2_df).append(tmp3_df).append(tmp4_df).append(tmp5_df).append(tmp6_df).append(tmp7_df)  # смена исполнителя
df_res_log_1 = pd.concat([df_res_log, tmp2_df])  
df_res_log_2 = pd.concat([df_res_log_1, tmp3_df])
df_res_log_3 = pd.concat([df_res_log_2, tmp4_df])
df_res_log_4 = pd.concat([df_res_log_3, tmp5_df])
df_res_log_5 = pd.concat([df_res_log_4, tmp6_df])
df_res_log_full = pd.concat([df_res_log_5, tmp7_df])
# сортировка
df_res_log_full = df_res_log_full.sort_values(by=['case_id','timestamp','event_id'])

with pd.ExcelWriter(file_in, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_res_log_full.to_excel(writer, sheet_name='event_log_py', index=False)


    





