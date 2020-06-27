#' 查询油卡
#'
#' @param conn 连接
#' @param FKeyWord 关键词
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oildCard_selectDB()
oildCard_selectDB <- function(conn=tsda::conn_rds('nsic'),FKeyWord='ljiang1469') {
  sql <- paste0("SELECT FOrderSouce  订单来源渠道
      ,FTBId  淘宝ID
      ,FOrderId 订单号
      ,FLiYu 礼遇

      ,FDealerName 经销商名称
      ,FOrderPhone  拍单手机号
      ,FCar 车型

      ,FTmallOrderTime  天猫下单时间
      ,FLMSStatus  LMS下发状态
      ,FLMSOrderTime  LMS下单时间
      ,FLMSNewStatus  LMS最新状态
      ,FChannelSource 渠道来源
      ,FVIN 车架号
      ,FVerificationStatus 核销情况
      ,FTmallOrderTimeBeforeLMS  天猫下单时间早于LMS下单时间
      ,FJudgeFavorableComments 是否好评
      ,FJudgeRules_Cause 是否符合礼遇领取规则
      ,FExtendGiftTime 礼包预计发放时间
      ,FExtendGiftDelivery 礼包发放单号
      ,Faddress 收货地址

  FROM  t_ic_oilCard
  where FTBId = '",FKeyWord,"' or FOrderId ='",FKeyWord,"' or FOrderPhone ='",FKeyWord,"'")
  #print()
  res <- tsda::sql_select(conn,sql)
  return(res)

}
#' 查询油卡数据
#'
#' @param conn  连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oildCard_selectDB_all()
oildCard_selectDB_all <- function(conn=tsda::conn_rds('nsic')) {
  sql <- paste0("SELECT FOrderSouce  订单来源渠道
      ,FTBId  淘宝ID
      ,FOrderId 订单号
      ,FLiYu 礼遇

      ,FDealerName 经销商名称
      ,FOrderPhone  拍单手机号
      ,FCar 车型

      ,FTmallOrderTime  天猫下单时间
      ,FLMSStatus  LMS下发状态
      ,FLMSOrderTime  LMS下单时间
      ,FLMSNewStatus  LMS最新状态
      ,FChannelSource 渠道来源
      ,FVIN 车架号
      ,FVerificationStatus 核销情况
      ,FTmallOrderTimeBeforeLMS  天猫下单时间早于LMS下单时间
      ,FJudgeFavorableComments 是否好评
      ,FJudgeRules_Cause 是否符合礼遇领取规则
      ,FExtendGiftTime 礼包预计发放时间
      ,FExtendGiftDelivery 礼包发放单号
      ,Faddress 收货地址

  FROM  t_ic_oilCard")
  #print()
  res <- tsda::sql_select(conn,sql)
  return(res)

}

#' 增值油卡订单查询功能
#'
#' @param conn 连接
#' @param FKeyWord 关键词
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oildCard_selectDB2()
oildCard_selectDB2 <- function(conn=tsda::conn_rds('nsic'),FKeyWord='ljiang1469') {
  sql <- paste0("SELECT
      FOrderId , FExtendGiftDelivery  FROM  t_ic_oilCard
  where FTBId = '",FKeyWord,"' or FOrderId ='",FKeyWord,"' or FOrderPhone ='",FKeyWord,"'")
  #print()
  data <- tsda::sql_select(conn,sql)
  ncount <- nrow(data)
  if(ncount >0){
    if(is.na(data$FExtendGiftDelivery)){
      #未单号
      msg <- paste0("亲，经查询，您的订单号：",data$FOrderId,"，目前礼包未发放,我们将尽快处理，请稍等")
    }else{
      msg <- paste0("亲，经查询，您的订单号：",data$FOrderId,"，目前礼包已发放，物流单号为：",data$FExtendGiftDelivery)

    }


  }else{
    msg <- paste0("亲，您的订单我们正在核实,请稍等")

  }


  return(msg)

}


#' 读取油卡数据
#'
#' @param file 文件名
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oilCard_readExcel()
oilCard_readExcel <- function(file="data-raw/oilCardData.xlsx") {
  #library(readxl)
  oilCardData <- readxl::read_excel(file, col_types = c("text", "text", "text",
                                                        "text", "text", "text", "text", "text",
                                                        "text", "text", "text", "text", "text",
                                                        "text", "text", "text", "text", "text",
                                                        "text", "text", "text", "text", "text",
                                                        "text", "text", "text"))
  #选择相应的数据
  data <-oilCardData[ ,1:25]
  #针对列进行重命名
  col_names <- c('FOrderSouce',
                  'FTBId',
                  'FOrderId',
                  'FLiYu',
                  'FDealerID',
                  'FDealerProvince',
                  'FDealerCity',
                  'FDealerName',
                  'FOrderPhone',
                  'FCar',
                  'FImportLocal',
                  'FTmallOrderTime',
                  'FLMSStatus',
                  'FLMSOrderTime',
                  'FLMSNewStatus',
                  'FChannelSource',
                  'FVIN',
                  'FVerificationStatus',
                  'FTmallOrderTimeBeforeLMS',
                  'FJudgeFavorableComments',
                  'FJudgeRules_Cause',
                  'FExtendGiftTime',
                  'FExtendGiftDelivery',
                  'Faddress',
                  'FRemarks')
 names(data) <- col_names

  return(data)


}





#' 删除库中数据
#'
#' @param conn 连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oilCard_backup_del()
oilCard_backup_del <- function(conn=tsda::conn_rds('nsic')) {
  sql_bak <- paste0("INSERT INTO [dbo].[t_ic_oilCardDel]
           ([FOrderSouce]
           ,[FTBId]
           ,[FOrderId]
           ,[FLiYu]
           ,[FDealerID]
           ,[FDealerProvince]
           ,[FDealerCity]
           ,[FDealerName]
           ,[FOrderPhone]
           ,[FCar]
           ,[FImportLocal]
           ,[FTmallOrderTime]
           ,[FLMSStatus]
           ,[FLMSOrderTime]
           ,[FLMSNewStatus]
           ,[FChannelSource]
           ,[FVIN]
           ,[FVerificationStatus]
           ,[FTmallOrderTimeBeforeLMS]
           ,[FJudgeFavorableComments]
           ,[FJudgeRules_Cause]
           ,[FExtendGiftTime]
           ,[FExtendGiftDelivery]
           ,[Faddress]
           ,[FRemarks]
           )
select * from t_ic_oilCard ")
  tsda::sql_update(conn,sql_bak)
  #删除数据
  sql_del <- paste0("delete from t_ic_oilCard")
  tsda::sql_update(conn,sql_del)

}


#' 油卡数据写入数据库
#'
#' @param file 文件
#' @param conn  连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' oilCard_writeDB()
oilCard_writeDB <- function(file="data-raw/oilCardData.xlsx",conn=tsda::conn_rds('nsic')){
  #删除库存数据
  oilCard_backup_del(conn=conn)

  #查询数据
  data <- oilCard_readExcel(file=file)
  #写入数据库
  tsda::db_writeTable(conn=conn,table_name = 't_ic_oilCard',r_object = data,append = TRUE)

}
