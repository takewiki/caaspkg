#' 查询用户信息
#'
#' @param conn 连接信息
#' @param app_id 程序名称
#'
#' @return 返回值
#' @import tsda
#' @export
#'
#' @examples
#' getUserInfoRpt
getUserInfoRpt  <- function(conn=conn_rds("rdbe"),app_id='caas') {
  sql <- paste0("select a.Fuser as 用户名,Fpermissions 角色,b.Fvalue 备注 from t_md_userRight a
inner join t_md_userInfo b
on a.FappId = b.FappId and a.Fuser = b.Fuser

where a.FappId ='",app_id,"'
and a.Fuser not in ('admin','demo')
and b.Fkey ='fcompany'")
  r <- tsda::sql_select(conn,sql)
  return(r)

}


#' 判断新用户
#'
#' @param conn 连接
#' @param app_id 程序
#' @param users 用户
#'
#' @return 返回逻辑值
#' @export
#'
#' @examples
#' getNewUsers()
getNewUsers <- function(conn=conn_rds('rdbe'),app_id='caas',users){
  sql <- paste0("select Fuser from t_md_userRight where FappId = '",app_id,"'")
  r <- tsda::sql_select(conn,sql)
  data <- r$Fuser
  res <- ! users %in% data
  return(res)
  }
