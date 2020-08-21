#' 查询客服报表
#'
#' @param FStartDate 开始日期
#' @param FEndDate  结束日期
#' @param conn 连接
#' @param FUser 导购员
#'
#' @return 返回数据
#' @import tsda
#' @export
#'
#' @examples
#' getCspRpt()
getQianNiuRpt <- function(conn=conn_rds_nsic(),FStartDate='2020-04-28',FEndDate='2020-05-07',FUser='demo'){
  sql <- paste0("select log_date as 日期,log_time  as 时间,author  as 对象 ,content  as 会话,FUser 用户,FUploadDate 上传日志,FIsA  品牌方 from vw_kf_log
where log_date >= '",FStartDate,"' and log_date <='",FEndDate,"' and FUser = '",FUser,"'")
  r <- tsda::sql_select(conn,sql)
  return(r)

}


#' 查询客服报表
#'
#' @param FStartDate 开始日期
#' @param FEndDate  结束日期
#' @param conn 连接
#'
#' @return 返回数据
#' @import tsda
#' @export
#'
#' @examples
#' getCspRpt()
getQianNiu2Rpt <- function(conn=conn_rds_nsic(),FStartDate='2020-04-28',FEndDate='2020-05-07'){
  sql <- paste0("
select FUser as 导购,FDate as 日期,FTotalCount as 记录数, FQuesCount as Q数, FAnswCount as A数,FBotCount as 平台数, FBotRatio as [使用率(%)]   from  t_ic_statALL
where FDate >='",FStartDate,"' and FDate <='",FEndDate,"' order by FDate desc,FUser asc")
  r <- tsda::sql_select(conn,sql)
  return(r)

}



#' 查询客服报表
#'
#' @param FStartDate 开始日期
#' @param FEndDate  结束日期
#' @param conn 连接
#'
#' @return 返回数据
#' @import tsda
#' @export
#'
#' @examples
#' getQianNiu2Rpt2()
getQianNiu2Rpt2 <- function(conn=conn_rds_nsic(),FStartDate='2020-04-28',FEndDate='2020-05-07'){
  sql <- paste0("
select FUser as 导购,FDate as 日期,FTotalCount as 记录数, FQuesCount as Q数, FAnswCount as A数,FBotCount as 平台数, FBotRatio as [使用率(%)]   from  t_ic_statALL2
where FDate >='",FStartDate,"' and FDate <='",FEndDate,"' order by FDate desc,FUser asc")
  r <- tsda::sql_select(conn,sql)
  return(r)

}

#' 判断是否新的千牛日志
#'
#' @param conn 连接
#' @param FUser 用户
#' @param FDate 日期
#'
#' @return 返回是否新的日志
#' @export
#'
#' @examples
#' is_new_QianNiu_Log()
is_newQianNiu_log <- function(conn=conn_rds_nsic(),FUser='admin',FDate='2020-05-01') {
  sql <- paste0("select top 1 * from t_ic_statALL
where FUser ='",FUser,"'and FDate='",FDate,"'")
  r<- tsda::sql_select(conn,sql)
  ncount <- nrow(r)
  if(ncount ==0 ){
    res <- TRUE
  }else{
    res <- FALSE
  }

  return(res)


}


#' 查询千牛日志按导购人员
#'
#' @param conn 连接
#' @param FUser 用户
#' @param FStartDate 开始日期
#' @param FEndDate  结束日期
#'
#' @return 返回数据框
#' @export
#'
#' @examples
#' getQN_log_byCspName()
getQN_log_byCspName <- function(conn=conn_rds_nsic(),FUser='admin',FStartDate='2020-04-28',FEndDate='2020-05-07') {
  sql <- paste0("select  FUser,FDate,FTotalCount  from t_ic_statALL
where FUser ='",FUser,"' and FDate >='",FStartDate,"' and FDate <='",FEndDate,"'
order by FDate desc")
  r <- tsda::sql_select(conn,sql)

  return(r)

}

#' 返回日志指定日期记录条数
#'
#' @param conn 连接
#' @param FUser 用户
#' @param FDate 日期
#'
#' @return 返回值
#' @export
#'
#' @examples
#' getQN_logCount_byCspName()
getQN_logCount_byCspName <- function(conn=conn_rds_nsic(),FUser='admin',FDate='2020-05-01') {
  sql <- paste0("
select  FTotalCount  from t_ic_statALL
where FUser ='",FUser,"' and FDate ='",FDate,"'

")
  r <- tsda::sql_select(conn,sql)
  ncount <- nrow(r)
  if(ncount >0){
    res <- r$FTotalCount
  }else{
    res <- 0
  }

  return(res)

}




#' 针对日志处理QA对
#'
#' @param conn 连接
#' @param FUser 用户
#' @param log_date 日志
#'
#' @return 返回值
#' @export
#'
#' @examples
#' log_compressIntoQA()
log_compressIntoQA<- function(conn=conn_rds_nsic(),FUser ='腊梅',log_date ='2020-08-19') {

  #针对数据处理处理
  sql_1 <- paste0("select 1  from t_kf_logGroup
where FUser ='",FUser,"' and  log_date ='",log_date,"'")
  r1 <- tsda::sql_select(conn,sql_1)
  ncount1 <- nrow(r1)
  if(ncount1 >0){
    #删除数据
    sql_del <- paste0("delete  from t_kf_logGroup
where FUser ='",FUser,"' and  log_date ='",log_date,"'")
     tsda::sql_update(conn,sql_del)
  }

  sql_sel <- paste0("select FUser,log_date,FCumFlag,FIsA,author, FLog from vw_kf_log
where FUser ='",FUser,"' and log_date='",log_date,"'
order by FCumFlag")
  r <- tsda::sql_select(conn,sql_sel)
  ncount <- nrow(r)
  if(ncount >0){
    my_seq <- r$FIsA
    my_seq[my_seq == 'FALSE'] <-'Q'
    my_seq[my_seq == 'TRUE'] <- 'A'
    r$FGroupId <- qa_getGroupId(my_seq)
    #上传数据
    tsda::db_writeTable(conn=conn,table_name = 't_kf_logGroup',r_object = r,append = T)

  }
  return(r)

}


#' 针对QA进行分组
#'
#' @param my_seq
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qa_getGroupId()
qa_getGroupId <- function(my_seq){
  ncount <- length(my_seq)
  res <- integer(ncount)
  if( my_seq[1] =='Q'){
    res[1] <- 1
  }else{
    res[1] <-0
  }
  for (i in 2:ncount) {
    if(my_seq[i] == 'A'){
      res[i] = res[i-1]
    }else{
      #分情况情况
      if(my_seq[i-1] =='Q'){
        res[i] = res[i-1]
      }else{
        res[i] = res[i-1] +1
      }
    }

  }
  return(res)

}


#' 根据日期获取用户
#'
#' @param conn 连接
#' @param log_date 日志日期
#'
#' @return 返回值
#' @export
#'
#' @examples
#' log_getUserListByDate()
log_getUserListByDate <- function(conn=conn_rds_nsic(),log_date ='2020-08-19') {
  sql_sel <- paste0("select  distinct  FUser from vw_kf_log where log_date='",log_date,"'")
  r <- tsda::sql_select(conn,sql_sel)
  ncount <- nrow(r)
  if(ncount >0){
    res <- r$FUser
  }else{
    res<-NA
  }
  return(res)

}


#' 按日期处理QA表
#'
#' @param conn  连接
#' @param log_date 日期
#'
#' @return 返回值
#' @export
#'
#' @examples
#' log_qaPair_byDate()
log_qaPair_byDate <- function(conn=conn_rds_nsic(),log_date ='2020-08-19'){
  users <- log_getUserListByDate(conn=conn,log_date = log_date)
  if(!is.na(users)){
   lapply(users, function(user){
      log_compressIntoQA(conn=conn,FUser = user,log_date = log_date)
    })
  }


}
