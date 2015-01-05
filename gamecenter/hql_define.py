class HQL:
    def __init__(self):
        self.dreverse = "create temporary function dreverse  as 'vivo.hadoop.project.Dreverse' "
        self.dstrcount= "create temporary function dstrcount as 'vivo.hadoop.project.Dstrcount'"
        self.uselog = " use log "
        self.addgcpart = """
            alter table gamecenter add if not exists partition(dt="%s") location "/gamecenter/%s/%s/%s"
        """
        self.insertfilter = """
            insert overwrite table filter partition(dt = "%s" , bussiness = "gamecenter")
            select action , req_date, pstr
            from gamecenter
            where  type != "HEADER" and action != "None"
            and action != "detectGamesUpdate" and dt="%s"
        """
        self.updateactive  = """
            from(
            from
                filter
            select
                pstr["imei"] as identity  ,
                pstr["model"] as model ,
                "1" as check,
                min(req_date) as first_login,
                max(req_date) as last_login,
                concat("&" , sum(if(pstr["origin"] == "20"  , 1 , 0))) as login_trace,
                concat("&" , max(pstr["appVersion"])) as version_trace
            where
                dt = "%s" and pstr["imei"] is not null and  pstr["model"] is not null and pstr["origin"] is not null
                and  pstr["imei"] != "" and pstr["model"] != "" and pstr["origin"] != ""
            group by
                pstr["imei"], pstr["model"]
            union all
            select
                identity,
                model,
                "2" as check,
                first_login,
                last_login,
                login_trace,
                version_trace
            from
                theme.active
            where
                dt="%s" and  bussiness="gamecenter"
            )t1
            insert
                overwrite table theme.active partition (dt="%s" , bussiness = "gamecenter")
            select
                identity,
                model,
                min(first_login) as first_login,
                max(last_login) as last_login,
                if(count(*)==2 ,
                    concat(max(login_trace) , min(login_trace)),
                    if (max(check)=="2",
                        concat(max(login_trace) , "&miss"),
                        split(max(login_trace) , "&")[1])
                        ) as login_trace,
                if(count(*)==2 ,
                    concat(max(version_trace) ,  min(version_trace)),
                    if (max(check)=="2",
                        concat(max(version_trace) , "&miss"),
                        split(max(version_trace),"&")[1])
                        ) as version_trace
            where
                login_trace is not null  and version_trace != ""
            group by
                identity , model
        """

        #retain and active info
        self.active= """
            select
            "%s"                                                                                 as dt ,
            count(*)                                                                             as total_num,
            sum(cast(split(dreverse(login_trace) , "&")[0] as float))                            as start_num,
            sum(if(to_date(last_login)=="%s" , 1, 0))                                            as dau,
            sum(if(to_date(last_login)=="%s" and to_date(first_login)=="%s",
                cast(split(dreverse(login_trace) , "&")[0] as float) , 0))                       as start_num,
            sum(if(to_date(last_login)=="%s" and to_date(first_login)=="%s", 1, 0))              as dau_new,

            sum(if(to_date(first_login) == date_sub("%s" , 1) and
                split(dreverse(login_trace),"&")[0] != "miss", 1.0 , 0.0))/
            sum(if(to_date(first_login) ==  date_sub("%s" , 1), 1.0 , 0.0))                      as N_retain_1,
            sum(if(to_date(first_login) == date_sub("%s" , 3) and
                split(dreverse(login_trace),"&")[0] != "miss", 1.0 , 0.0) )/
            sum(if(to_date(first_login) ==  date_sub("%s" , 3), 1.0 , 0.0))                      as N_retain_4,
            sum(if(to_date(first_login) == date_sub("%s" , 7) and
                 split(dreverse(login_trace),"&")[0] != "miss", 1.0 , 0.0) )/
            sum(if(to_date(first_login) ==  date_sub("%s" , 7), 1.0 , 0))                        as N_retain_8,
            sum(if(to_date(first_login) == date_sub("%s" , 30) and
                 split(dreverse(login_trace),"&")[0] != "miss", 1.0 , 0.0) )/
            sum(if(to_date(first_login) ==  date_sub("%s" , 30), 1.0 , 0.0))                     as N_retain_31,

            sum(if(split(dreverse(login_trace),"&")[1] != "miss" and
                   split(dreverse(login_trace),"&")[0] != "miss", 1.0, 0.0))/
            sum(if(split(dreverse(login_trace),"&")[1] != "miss",1.0,0.0))                       as A_retain_1,
            sum(if(split(dreverse(login_trace),"&")[3] != "miss" and
                   split(dreverse(login_trace),"&")[0] != "miss", 1.0, 0.0))/
            sum(if(split(dreverse(login_trace),"&")[3] != "miss",1.0,0.0))                       as A_retain_4,
            sum(if(split(dreverse(login_trace),"&")[7] != "miss" and
                   split(dreverse(login_trace),"&")[0] != "miss", 1.0, 0.0))/
            sum(if(split(dreverse(login_trace),"&")[7] != "miss",1.0,0.0))                       as A_retain_8,
            sum(if(split(dreverse(login_trace),"&")[30] != "miss" and
                   split(dreverse(login_trace),"&")[0] != "miss", 1.0, 0.0))/
            sum(if(split(dreverse(login_trace),"&")[30] != "miss",1.0,0.0))                      as A_retain_31
        from
            theme.active
        where
             dt="%s" and bussiness = "gamecenter"
        """
        # download  cancel_download   install info
        self.download = """
            from(
                from(from(select
                            pstr["imei"] as identity ,
                            pstr["id"] as id,
                            sum(if(action == "gameDownload" , 1, 0)) as d,
                            sum(if(action == "gameDownload" and pstr["type"] == "1" , 1, 0)) as df,
                            sum(if(action == "gameDownload" and pstr["type"] == "2" , 1, 0)) as du,
                            sum(if(action == "downloadCancellation" , 1, 0)) as cl,
                            sum(if(action == "gameInstall" , 1, 0)) as ins
                        from
                            log.filter
                        where
                            dt = "%s" and  bussiness = "gamecenter" and pstr["imei"] != ""
                            and pstr["id"] != "" and
                            (action == "gameDownload" or action == "downloadCancellation" or action == "gameInstall")
                        group by
                            pstr["imei"] ,pstr["id"]
                        )t1
                    select
                        identity,
                        sum(d) as d,
                        sum(if(d !=0 , 1 , 0))   as d_id,
                        sum(df) as df,
                        sum(if(df !=0 , 1 , 0))  as df_id,
                        sum(du) as du,
                        sum(if(du !=0 , 1 , 0))  as du_id,
                        sum(cl) as cl,
                        sum(if(cl !=0 , 1 ,  0)) as cl_id,
                        sum(ins) as ins,
                        sum(if(ins !=0 , 1 , 0)) as ins_id
                    group by
                        identity
                    )t2
                   left outer join

                   (select distinct identity  , "new" as check
                    from  theme.active
                    where
                    dt = "%s" and  bussiness = "gamecenter" and to_date(first_login)=="%s"
                   )t3
                   on t2.identity == t3.identity
                select
                   t2.identity,
                   t2.d,
                   t2.d_id,
                   t2.df,
                   t2.df_id,
                   t2.du,
                   t2.du_id,
                   t2.cl,
                   t2.cl_id,
                   t2.ins,
                   t2.ins_id,
                   check
            )t4
            select
                sum(d) as d,
                sum(d_id) as d_id,
                sum(if(d !=0 , 1 , 0)) as d_imei,
                sum(df) as df,
                sum(df_id) as df_id,
                sum(if(df !=0 , 1 , 0)) df_imei,
                sum(du) as du,
                sum(du_id) as du_id,
                sum(if(du !=0 , 1 , 0)) as du_imei,
                sum(cl) as cl,
                sum(cl_id) as cl_id,
                sum(if(cl !=0 , 1 , 0)) as cl_imei,
                sum(ins) as ins,
                sum(ins_id) as ins_id,
                sum(if(ins !=0 ,1,0)) as ins_imei,
                sum(if(check is null , d , 0)) as d_old,
                sum(if(check is null  and d !=0, 1 , 0)) as d_imei_old,
                sum(if(check is null , df_id , 0)) as df_id_old,
                sum(if(check is null  and df !=0 , 1 , 0)) as df_imei_old
        """

        #start num and download num (by id) distribution
        self.spread="""
             from(select
                         from_unixtime(unix_timestamp(req_date)-unix_timestamp(req_date)%%1800 , "HH:mm") as time,
                         if(action == "gameDownload" , concat(pstr["imei"] , pstr["id"]) , "no" ) as identitiy,
                         sum(if(pstr["origin"]=="20" , 1, 0)) as start ,
                         count(*) as d_id
                    from log.filter
                    where dt= "%s" and  bussiness="gamecenter"
                    group by
                         from_unixtime(unix_timestamp(req_date)-unix_timestamp(req_date)%%1800 , "HH:mm") ,
                         if(action == "gameDownload" , concat(pstr["imei"] , pstr["id"]) , "no" )
                    )t1
             select  max("%s") ,  time , sum(start) as start , sum(if(identitiy != "no" , d_id, 0)) as d_id
             group by time
        """
        #version retain today_info
        self.version_retain_i = """
                select
                    max("%s"),
                    split(dreverse(version_trace) ,"&")[0] as version,
                    count(*) as user
                from
                    theme.active
                where dt="%s" and bussiness = "gamecenter" and
                    split(dreverse(version_trace) , "&")[0] != "miss"
                group by split(dreverse(version_trace) , "&")[0]
        """
        #version retain update previous day
        self.version_retain_u="""
            select
                sum( if(split(dreverse(version_trace) , "&")[0] != "miss"
                     and split(dreverse(version_trace) , "&")[%s] !="miss" , 1.0 , 0.0))/
                sum( if(split(dreverse(login_trace) , "&")[%s] != "miss" , 1.0 , 0.0))             as ratio,
                split(dreverse(version_trace) ,"&")[%s]                                            as version
            from
                theme.active
            where dt="%s" and bussiness = "gamecenter"
                and split(dreverse(version_trace) ,"&")[%s] is not null
            group by split(dreverse(version_trace) , "&")[%s]
        """
        #caculate every sven day retain and ervery 30days retain
        self.zoneretain = """
            select
            sum(if(dstrcount(login_trace , 0 , 6 )!=0 and dstrcount(login_trace , 7 , 13)!=0 , 1, 0)),
            sum(if(dstrcount(login_trace , 7 , 13)!=0 , 1, 0)),
            sum(if(dstrcount(login_trace , 0 , 29 )!=0 and dstrcount(login_trace , 30 , 59)!=0 , 1, 0)),
            sum(if(dstrcount(login_trace , 30 , 59)!=0 , 1, 0))
            from theme.active
            where dt="%s" and bussiness = "gamecenter"
        """
        #for week active download report
        self.month_info = """
            from
                (from
                    (from
                        (select
                            pstr["imei"] as imei,
                            if(pstr["id"] is not null , pstr["id"] , concat("miss" , rand())) as id,
                            if(action=="gameDownload" , 1 , 0) as d,
                            if(action=="gameDownload" and pstr["type"] == "1" , 1 , 0) as df,
                            if(action=="gameDownload" and pstr["type"] == "2" , 1 , 0) as du
                        from log.filter
                        where dt<="%s" and dt >= "%s"
                        and bussiness = "gamecenter"
                        )t1
                    select
                        imei,
                        id,
                        sum(d) as d,
                        sum(du) as du,
                        sum(df) as df
                    group by imei , id
                    )t2
                select
                    imei,
                    sum(d) as d,
                    sum(du) as du,
                    sum(if(d != 0 , 1 ,0)) as dau_id,
                    sum(if(df != 0 , 1, 0)) as df
                group by imei
                )t3
            select
                count(*) as dau,
                sum(d) as d,
                sum(du) as du,
                sum(dau_id) as dau_id,
                sum(if(df != 0 , 1 , 0 )) as df
        """
        self.login_spread = """
        from(
             select
             identity,
             dstrcount(login_trace , 0 , 29) as logins
             from theme.active
             where dt = "%s" and bussiness = "gamecenter"
             and dstrcount(login_trace , 0 , 29) != 0
        )t1
        select
        sum(if(logins == 30  , 1 ,0))/count(identity) as p_30,
        sum(if(logins == 29  , 1 ,0))/count(identity) as p_29,
        sum(if(logins == 28  , 1 ,0))/count(identity) as p_28,
        sum(if(logins == 27  , 1 ,0))/count(identity) as p_27,
        sum(if(logins == 26  , 1 ,0))/count(identity) as p_26,
        sum(if(logins == 25  , 1 ,0))/count(identity) as p_25,
        sum(if(logins == 24  , 1 ,0))/count(identity) as p_24,
        sum(if(logins == 23  , 1 ,0))/count(identity) as p_23,
        sum(if(logins == 22  , 1 ,0))/count(identity) as p_22,
        sum(if(logins == 21  , 1 ,0))/count(identity) as p_21,
        sum(if(logins == 20  , 1 ,0))/count(identity) as p_20,
        sum(if(logins == 19  , 1 ,0))/count(identity) as p_19,
        sum(if(logins == 18  , 1 ,0))/count(identity) as p_18,
        sum(if(logins == 17  , 1 ,0))/count(identity) as p_17,
        sum(if(logins == 16  , 1 ,0))/count(identity) as p_16,
        sum(if(logins == 15  , 1 ,0))/count(identity) as p_15,
        sum(if(logins == 14  , 1 ,0))/count(identity) as p_14,
        sum(if(logins == 13  , 1 ,0))/count(identity) as p_13,
        sum(if(logins == 12  , 1 ,0))/count(identity) as p_12,
        sum(if(logins == 11  , 1 ,0))/count(identity) as p_11,
        sum(if(logins == 10  , 1 ,0))/count(identity) as p_10,
        sum(if(logins == 9   , 1 ,0))/count(identity) as p_9 ,
        sum(if(logins == 8   , 1 ,0))/count(identity) as p_8 ,
        sum(if(logins == 7   , 1 ,0))/count(identity) as p_7 ,
        sum(if(logins == 6   , 1 ,0))/count(identity) as p_6 ,
        sum(if(logins == 5   , 1 ,0))/count(identity) as p_5 ,
        sum(if(logins == 4   , 1 ,0))/count(identity) as p_4 ,
        sum(if(logins == 3   , 1 ,0))/count(identity) as p_3 ,
        sum(if(logins == 2   , 1 ,0))/count(identity) as p_2 ,
        sum(if(logins == 1   , 1 ,0))/count(identity) as p_1
        """

if __name__=='__main__':
    pass

