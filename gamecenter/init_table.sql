 use test;
drop table if exists daily_active;
create table if not exists  daily_active(
    dt          date  comment '统计日期',
    user_h      int   comment 'user in history ,历史总用户',
    start_t     int   comment 'total start num , 启动总次数',
    dau         int   comment 'daily active user, 活跃用户',
    start_n     int   comment 'new user start gamecent number ,新用户启动次数',
    dnu         int   comment 'new active user , 新活跃用户',
    dau_old     int   comment 'old active user , 老活跃用户',
    a_percent   float comment ' active user percent in history user , 活跃用户占总用户比例',
    retain_n_1  float comment 'new user retain after one day ,新用户次日留存',
    retain_n_3  float comment 'new user retain after three day ,新用户三日留存',
    retain_n_7  float comment 'new user retain after seven day ,新用户七日留存',
    retain_n_30 float comment 'new user retain after 30  day ,新用户30日留存',
    retain_a_1  float comment 'active user retain after one day ,活跃用户次日留存',
    retain_a_3  float comment 'active user retain after three  day ,活跃用户三日留存',
    retain_a_7  float comment 'active user retain after seven day ,活跃用户七日留存',
    retain_a_30 float comment 'active user retain after 30  day ,活跃用户30日留存'
);

drop table if exists download;
create table if not exists  download(
    dt            date  comment 'statistic day , 统计日期',
    d             int   comment 'download num',
    d_id          int   comment 'download num identified by  imei_id ',
    d_imei        int   comment 'download num of user identified by imei',
    df            int   comment 'first download num ,refer to type = 1',
    df_id         int   comment 'first download num identified by imei_id ,refer to type =1',
    df_imei       int   comment 'first download num of user ,identified by imei .refer to type =1',
    du            int   comment 'download num for update , refer to type = 2',
    du_id         int   comment 'download num for update identified by  imei_id ,refer to type =2',
    du_imei       int   comment 'download user for update',
    cl            int   comment 'num of cancel the download process ',
    cl_id         int   comment 'num of cancel the download process ,identified by imei_id',
    cl_imei       int   comment 'user of cancel the download process',
    ins           int   comment 'install num',
    ins_id        int   comment 'install num identified by imei_id',
    ins_imei      int   comment 'install user identified by imei',
    d_old         int   comment 'download num belong to old user ',
    d_imei_old    int   comment 'download user belong to old user',
    df_id_old     int   comment 'download(type=1) num identified by imei_id belong to old user',
    df_imei_old   int   comment 'download (type=1) user identified by imei belong to old user'
);

drop table if exists spread;
create table if not exists  spread(
    dt            date  comment 'statistic day , 统计日期',
    hour          varchar(10)   comment 'download num',
    start         int   comment 'start_num ',
    download      int   comment 'download num  identified by imei_id'
    );



drop table if exists version_retain;
create table if not exists  version_retain(
    dt            date          comment 'statistic day , 统计日期',
    version       varchar(10)   comment 'version name ',
    dau           int           comment 'version daily active user ',
    retain_1      float         comment 'retain user in the version afer 1 day ',
    retain_3      float         comment 'retain user in the version afer 3 day ',
    retain_7      float         comment 'retain user in the version afer 7 day ',
    retain_30     float         comment 'retain user in the version afer 30 day '
    );
drop table if exists zoneretain;
create table if not exists  zoneretain(
    dt                  date          comment 'statistic day , 统计日期',
    c_week              int           comment 'user who have login record in previous week login current week ',
    p_week              int           comment 'previous week active user ',
    week_percent        float         comment 'week retain percent',
    c_month             int           comment 'user who have login record in previous month login current month ',
    p_month             int           comment 'previous month active user ',
    month_percent       float         comment 'month retain percent'
    );

drop table if exists month_info;
create table if not exists  month_info(
    dt                  date          comment 'statistic day , 统计日期',
    mau                 int           comment 'active user in recent 30 days ',
    download            int           comment 'download num in last 30 days ',
    du                  int           comment 'update download num in last 30 days',
    d_id                int           comment 'download num identified by id ',
    df_imei             int           comment 'first download user in last 30 days '
    );

drop table if exists login_spread;
create table if not exists  login_spread(
     dt     date      comment 'statistic day , 统计日期',
    p_30    float     comment   ' percent of the user login 30  in last 30days ' ,
    p_29    float     comment   ' lprcent of the user login 29  in last 30days ' ,
    p_28    float     comment   ' lprcent of the user login 28  in last 30days ' ,
    p_27    float     comment   ' lprcent of the user login 27  in last 30days ' ,
    p_26    float     comment   ' lprcent of the user login 26  in last 30days ' ,
    p_25    float     comment   ' lprcent of the user login 25  in last 30days ' ,
    p_24    float     comment   ' lprcent of the user login 24  in last 30days ' ,
    p_23    float     comment   ' lprcent of the user login 23  in last 30days ' ,
    p_22    float     comment   ' lprcent of the user login 22  in last 30days ' ,
    p_21    float     comment   ' lprcent of the user login 21  in last 30days ' ,
    p_20    float     comment   ' lprcent of the user login 20  in last 30days ' ,
    p_19    float     comment   ' lprcent of the user login 19  in last 30days ' ,
    p_18    float     comment   ' lprcent of the user login 18  in last 30days ' ,
    p_17    float     comment   ' lprcent of the user login 17  in last 30days ' ,
    p_16    float     comment   ' lprcent of the user login 16  in last 30days ' ,
    p_15    float     comment   ' lprcent of the user login 15  in last 30days ' ,
    p_14    float     comment   ' lprcent of the user login 14  in last 30days ' ,
    p_13    float     comment   ' lprcent of the user login 13  in last 30days ' ,
    p_12    float     comment   ' lprcent of the user login 12  in last 30days ' ,
    p_11    float     comment   ' lprcent of the user login 11  in last 30days ' ,
    p_10    float     comment   ' lprcent of the user login 10  in last 30days ' ,
    p_9     float     comment   ' lprcent of the user login 9   in last 30days ' ,
    p_8     float     comment   ' lprcent of the user login 8   in last 30days ' ,
    p_7     float     comment   ' lprcent of the user login 7   in last 30days ' ,
    p_6     float     comment   ' lprcent of the user login 6   in last 30days ' ,
    p_5     float     comment   ' lprcent of the user login 5   in last 30days ' ,
    p_4     float     comment   ' lprcent of the user login 4   in last 30days ' ,
    p_3     float     comment   ' lprcent of the user login 3   in last 30days ' ,
    p_2     float     comment   ' lprcent of the user login 2   in last 30days ' ,
    p_1     float     comment   ' lprcent of the user login 1   in last 30days ' 
    );

