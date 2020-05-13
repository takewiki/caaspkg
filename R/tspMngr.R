#' 查询TSP结果
#'
#' @param conn 连接
#' @param FStartDate  开始
#' @param FEndDate 结束
#'
#' @return 返回值
#' @import tsda
#' @export
#'
#' @examples
#' getTspRpt()
getTspRpt <- function(conn=conn_rds_nsic(),FStartDate='2020-04-28',FEndDate='2020-05-07'){

  sql <- paste0("SELECT
      [FCreateDate] 创建日期
      ,[FCreateTime] 创建时间
      ,[FCspName] 导购姓名
	  ,b.Fvalue  as 备注
      ,[FQues]  问题

      ,[FPriorCount] 催单次数

  FROM [nsic].[dbo].[t_tsp_ques] a
  inner join  rdbe.dbo.t_md_userInfo b
  on a.FCspName =  b.Fuser
  where FCspName not in ('RDS','bot19','admin')
  and  FCreateDate >='",FStartDate,"' and FCreateDate <='",FEndDate,"'
  and b.Fkey ='Fcompany'")
  r <- tsda::sql_select(conn,sql)
  return(r)

}
