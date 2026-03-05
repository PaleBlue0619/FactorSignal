import pandas as pd
import dolphindb as ddb
from typing import Dict, List
from src.entity.Result import Result

class Eva(Result):
    def __init__(self, session: ddb.session):
        super().__init__(session)

    def eva(self, signalList: List[str]):
        self.session.upload({"factorList": signalList})
        """初始化定义"""
        startDate = pd.Timestamp(self.startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(self.endDate).strftime("%Y.%m.%d")
        self.session.run(rf"""
        // 参数配置
        startDate = {self.startDate}
        endDate = {self.endDate}
        callBackDays = 120
        afterStatDays = [3,4]
        barRetLabelName = "{self.barRetLabelName}"
        realStartDate = temporalAdd(startDate, -callBackDays, "d")
        realEndDate = endDate
        factorDB = "{self.factorDBName}"
        factorTB = "{self.factorTBName}"
        labelDB = "{self.labelDBName}"
        labelTB = "{self.labelTBName}"
        
        // 取数
        signalDF = select symbol,tradeDate,factor,value from loadTable(factorDB,factorTB) where factor in factorList and (tradeDate between realStartDate and realEndDate) 
        labelDF = select cont as symbol,tradeDate,value as ret from loadTable(labelDB,labelTB) where label == barRetLabelName and (tradeDate between realStartDate and realEndDate)
        signalDF = lj(signalDF, labelDF, `symbol`tradeDate)
        
        // 计算
        summaryStats1 = select symbol, tradeDate, factor, value, ret,
            callBackDays as `period, 0 as `after,
            mcount(iif(value == 1, 1, NULL), callBackDays) as posNum,
            mcount(iif(value == -1, 1, NULL), callBackDays) as negNum,
            mcount(iif(value == 0, 1, NULL), callBackDays) as zeroNum,
            signal = value == 1,
            condition2D = iif((move(ret,-1)>0 and move(ret,-2)>0), 1, 0),
            condition3D = iif((move(ret,-1)>0 and move(ret,-2)>0 and move(ret,-3)>0), 1, 0)
            from signalDF context by factor, symbol
            order by symbol, tradeDate
        
        update summaryStats1 set consUp2DRatePos = nullFill(msum(iif(condition2D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condition2D == 1 and value == 1, 1, 0), 2),0) context by factor, symbol
        update summaryStats1 set consUp3DRatePos = nullFill(msum(iif(condition3D == 1 and value == 1, 1, 0), callBackDays)-msum(iif(condition3D == 1 and value == 1, 1, 0), 3),0) context by factor, symbol
        """)