#处理分组合并的逻辑---------
#日志合同操作说明-------
#' 针对QA对进行合并
#'
#' @param conn 连接
#' @param FUser 用户
#' @param log_date 日期
#' @param sep 定义分隔符
#'
#' @return 返回日
#' @export
#'
#' @examples
#' log_qna_combined()
log_qna_combined <- function(conn=conn_rds_nsic(),FUser ='蓝淀',log_date ='2020-08-18',sep="|"){

  #读取input数据------
  sql <- paste0("select FUser,log_date,FCumFlag,FIsA,author,FLog,FGroupId  from t_kf_logGroup_Input
where FUser ='",FUser,"' and log_date ='",log_date,"'
order by FCumFlag,FGroupId ")
  mydata <- tsda::sql_select(conn,sql)
  #读取数据

  col_names <- names(mydata)
  #使用FGoupId+FIsA作为合并分组依据-------
  mydata$FLag2 <- paste0(as.character(mydata$FGroupId),mydata$FIsA)
  mydata_split <- split(mydata,mydata$FLag2)
  mydata_res <- lapply(mydata_split, function(data){
    res <- data[1, ]
    #重点字段------
    info <- data$FLog
    #针对Flog进行合并--------
    res$FLog <- paste0(info,collapse = sep)
    return(res)
  })
  res2 <- do.call("rbind",mydata_res)
  #完成数据合并--------
  res2 <- res2[order(res2$FGroupId),]
  res2 <- res2[ ,col_names]
  rownames(res2) <- NULL
  #删除已有数据------
  sql_sel <- paste0("select 1 from t_kf_logCombined
where FUser ='",FUser,"' and log_date ='",log_date,"'")
  res_check1 <- tsda::sql_select(conn,sql_sel)
  ncount_check1 <-nrow(res_check1)
  if(ncount_check1 >0){
    #删除已有的数据
    sql_del <- paste0("delete from t_kf_logCombined
where FUser ='",FUser,"' and log_date ='",log_date,"'")
    tsda::sql_update(conn,sql_del)

  }

  #处理ID处理
  #处理合并的内码-------
  #处理数据内码，Q与A各占一行
  sql_maxId <- paste0("select  isnull(max(FInterId),0) as FInterId  from t_kf_logCombined")
  res_maxId <- tsda::sql_select(conn,sql_maxId)
  value_maxId <- res_maxId$FInterId
  ncount_res2 <- nrow(res2)
  res2$FInterId <- 1:ncount_res2 + value_maxId
  res2$Fflag <- ''
  res2$Ftag_by <- ''
  res2$FIs_valid <- ''
  res2$FCategory <-''

  #进行分页处理，优化性能
  ncount_res2 <- nrow(res2)
  pages <- page_setting(ncount_res2,500)
  lapply(pages, function(page){
    tsda::db_writeTable(conn,table_name = 't_kf_logCombined',r_object = res2[page,],append = T)
  })



 return(res2)

}


#' 针对QA对进行合并3
#'
#' @param conn 连接
#' @param FUser 用户
#' @param log_date 日期
#' @param sep 定义分隔符
#'
#' @return 返回日
#' @export
#'
#' @examples
#' log_qna_combined3()
log_qna_combined3 <- function(conn=conn_rds_nsic(),FUser ='腊梅',log_date ='2020-08-19',sep="|"){

  #读取input数据------
  #针对数据进行处理，删除黑名单中的记录，其中数据用于合并
  # Fflag ='FALSE'为没有中标黑名单数据
  sql <- paste0("select FUser,log_date,FIsA,author,content as FLog,FGroupId from t_kf_logTag3
where  Fflag ='FALSE' and FUser ='",FUser,"' and log_date ='",log_date,"'
order by FGroupId ")
  mydata <- tsda::sql_select(conn,sql)
  ncount <- nrow(mydata)
  if(ncount >0){
    #读取数据
    print(mydata)
    print('s1')

    col_names <- names(mydata)
    #使用FGoupId+FIsA作为合并分组依据-------
    mydata$FLag2 <- paste0(as.character(mydata$FGroupId),mydata$FIsA)
    mydata_split <- split(mydata,mydata$FLag2)
    print('s2')
    mydata_res <- lapply(mydata_split, function(data){
      res <- data[1, ]
      #重点字段------
      info <- data$FLog
      #针对Flog进行合并--------
      res$FLog <- paste0(info,collapse = sep)
      return(res)
    })
    print('s3')
    res2 <- do.call("rbind",mydata_res)
    print('s4')
    #完成数据合并--------
    res2 <- res2[order(res2$FGroupId),]
    res2 <- res2[ ,col_names]
    rownames(res2) <- NULL
    #针对数据进行处理，删除只有A没有Q的数据
    AA <-res2$FGroupId[res2$FIsA == 'TRUE']
    QQ <-res2$FGroupId[res2$FIsA == 'FALSE']
    AA_DEL <-AA[!AA %in% QQ]
    QA_selected <- !res2$FGroupId %in% AA_DEL
    res2 <- res2[QA_selected,]
    #删除已有数据------
    sql_sel <- paste0("select 1 from t_kf_logCombined3
where FUser ='",FUser,"' and log_date ='",log_date,"'")
    res_check1 <- tsda::sql_select(conn,sql_sel)
    ncount_check1 <-nrow(res_check1)
    if(ncount_check1 >0){
      #删除已有的数据
      sql_del <- paste0("delete from t_kf_logCombined3
where FUser ='",FUser,"' and log_date ='",log_date,"'")
      tsda::sql_update(conn,sql_del)

    }

    #处理ID处理
    #处理合并的内码-------
    #处理数据内码，Q与A各占一行
    sql_maxId <- paste0("select  isnull(max(FInterId),0) as FInterId  from t_kf_logCombined3")
    res_maxId <- tsda::sql_select(conn,sql_maxId)
    value_maxId <- res_maxId$FInterId
    ncount_res2 <- nrow(res2)
    res2$FInterId <- 1:ncount_res2 + value_maxId
    res2$Fflag <- 'FALSE'
    res2$Ftag_by <- ''
    res2$FIs_valid <- ''
    res2$FCategory <-''

    #进行分页处理，优化性能
    ncount_res2 <- nrow(res2)
    pages <- page_setting(ncount_res2,500)
    lapply(pages, function(page){
      tsda::db_writeTable(conn,table_name = 't_kf_logCombined3',r_object = res2[page,],append = T)
    })



    return(res2)
  }


}


#' 按天获取相关的用户
#'
#' @param conn 连接
#' @param log_date 日志日期
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qna_getUserListByDate()
qna_getUserListByDate <- function(conn=conn_rds_nsic(),log_date ='2020-08-19') {
  sql_sel <- paste0("select  distinct  FUser from t_kf_logGroup_Input  where log_date='",log_date,"'")
  r <- tsda::sql_select(conn,sql_sel)
  ncount <- nrow(r)
  if(ncount >0){
    res <- r$FUser
  }else{
    res<-NA
  }
  return(res)

}

#' 按天获取相关的用户3
#'
#' @param conn 连接
#' @param log_date 日志日期
#'
#' @return 返回值
#' @export
#'
#' @examples
#' qna_getUserListByDate3()
qna_getUserListByDate3 <- function(conn=conn_rds_nsic(),log_date ='2020-08-19') {
  sql_sel <- paste0("select  distinct  FUser from t_kf_logTag3  where log_date='",log_date,"'")
  r <- tsda::sql_select(conn,sql_sel)
  ncount <- nrow(r)
  if(ncount >0){
    res <- r$FUser
  }else{
    res<-NA
  }
  return(res)

}

#step2合并数据------------
#' 按天对数据进行合并
#'
#' @param conn 连接
#' @param log_date 日期
#' @param sep 分隔符
#'
#' @return 返回值
#' @export
#'
#' @examples
#' log_qna_combined_byDate()
log_qna_combined_byDate <- function(conn=conn_rds_nsic(),log_date ='2020-08-19',sep="|"){
  users <- qna_getUserListByDate(conn=conn,log_date = log_date)
  if(!is.na(users)){
    lapply(users, function(user){
      print(user)
      log_qna_combined(conn=conn,FUser = user,log_date = log_date,)
    })
  }


}


#' 按天对数据进行合并3
#'
#' @param conn 连接
#' @param log_date 日期
#' @param sep 分隔符
#'
#' @return 返回值
#' @export
#'
#' @examples
#' log_qna_combined_byDate3()
log_qna_combined_byDate3 <- function(conn=conn_rds_nsic(),log_date ='2020-08-19',sep="|"){
  users <- qna_getUserListByDate3(conn=conn,log_date = log_date)
  if(!is.na(users)){
    lapply(users, function(user){
      print(user)
      log_qna_combined3(conn=conn,FUser = user,log_date = log_date,)
    })
  }


}

#' 针对日志数据进行打标更新
#'
#' @param conn 链接信息
#'
#' @return 返回值
#' @export
#'
#' @examples
#' log_qna_updatedLogTag()
log_qna_updatedLogTag <- function(conn=conn_rds_nsic()) {
  #更新数据
  sql_udp <-paste0("update a set a.Fflag = b.Fflag,a.Ftag_by = b.Ftag_by from t_kf_logCombined a
inner join t_kf_logTag2  b
on a.FInterId = b.FInterId")
  tsda::sql_update(conn,sql_udp)
  #删除相当的数据表
  sql_log_group <- paste0("truncate table t_kf_logGroup_Input")
  tsda::sql_update(conn=conn,sql_log_group)
  #删除打标表

}

#' 针对日志数据进行打标更新3
#'
#' @param conn 链接信息
#'
#' @return 返回值
#' @export
#'
#' @examples
#' log_qna_updatedLogTag3()
log_qna_updatedLogTag3 <- function(conn=conn_rds_nsic()) {
  #更新数据
#   sql_udp <-paste0("update a set a.Fflag = b.Fflag,a.Ftag_by = b.Ftag_by from t_kf_logCombined a
# inner join t_kf_logTag2  b
# on a.FInterId = b.FInterId")
#   tsda::sql_update(conn,sql_udp)
  #删除相当的数据表
  sql_log_group <- paste0("truncate table t_kf_logGroup_Input")
  tsda::sql_update(conn=conn,sql_log_group)
  #删除打标表

}
