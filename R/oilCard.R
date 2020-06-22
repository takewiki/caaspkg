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
