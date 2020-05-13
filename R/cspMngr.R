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
 getCspRpt <- function(conn=conn_rds_nsic(),FStartDate='2020-04-28',FEndDate='2020-05-07'){
  sql <- paste0("SELECT  [FNickName] as 导购名称
       ,  b.fvalue as 备注
      ,[FCreateTime] as 创建时间
	  ,left(FCreateTime,11) as 创建日期
      ,[FQuesText] 问题



      ,[FAnswerText] 答案
	   ,[FScore] 得分
	        ,[FAnswerType]  答案类型
      ,[FAnswerNumber] 答案选项

  FROM [nsic].[dbo].[t_ic_log]  a
  inner join rdbe.dbo.t_md_userInfo b
  on a.FNickName = b.Fuser

  where FCreateTime >='",FStartDate,"' and FCreateTime <='",FEndDate,"'
  and FNickName not in ('RDS','admin','demo')
  and b.Fkey ='Fcompany'
                ")
  r <- tsda::sql_select(conn,sql)
  return(r)

}
