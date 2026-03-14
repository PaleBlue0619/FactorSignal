import pandas as pd
import dolphindb as ddb
import streamlit as st
from functools import lru_cache
from typing import Dict, List
from src.entity.Source import Source

class Result(Source):
    def __init__(self, session: ddb.session):
        super().__init__(session)
        self.startDate: pd.Timestamp = None
        self.endDate: pd.Timestamp = None
        self.callBackDays: List[int] = []
        self.afterStatDays: List[List[int]] = []
        self.barRetLabelName: str = ""

    def setConfig(self, config: Dict):
        """初始化结果配置项"""
        self.startDate = pd.Timestamp(config["startDate"]) if config["startDate"] is not None else pd.Timestamp("20200101")
        self.endDate = pd.Timestamp(config["endDate"]) if config["endDate"] is not None else pd.Timestamp.now().date()
        self.callBackDays = config["callBackDays"]
        self.afterStatDays = config["afterStatDays"]
        self.barRetLabelName = config["barRetLabelName"]

    def deleteSignalRes(self, signalList: List[str]) -> None:
        """
        删除结果数据库中对应信号的结果
        """
        self.session.upload({"deleteSignals": signalList})
        self.session.run(f"""
        delete from loadTable("{self.resultDBName}", "{self.resultTBName}") where signal in deleteSignals;
        """)

    def initResDB(self, dropDB: bool = False) -> None:
        """
        创建结果数据库
        """
        if dropDB and self.session.existsDatabase(self.resultDBName):
            self.session.dropDatabase(self.resultDBName)
        if not self.session.existsTable(dbUrl=self.resultDBName, tableName=self.resultTBName):
            colName = ["symbol","tradeDate","signal","period","indicator","value"]
            colType = ["SYMBOL","DATE","SYMBOL","INT","SYMBOL","DOUBLE"]
            self.session.run(f"""
            db=database("{self.resultDBName}",VALUE,2015.01.01..2030.01.01,engine="OLAP")
            schemaTb=table(1:0,{colName}, {colType});
            t=db.createPartitionedTable(table=schemaTb, tableName="{self.resultTBName}", partitionColumns="tradeDate")
            """)  # DolphinDB 维度表 - 信号结果数据库

    def getSymbolList(self) -> List[str]:
        """
        获取当前结果数据库中标的列表
        """
        startDate = pd.Timestamp(self.startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(self.endDate).strftime("%Y.%m.%d")
        symbolDF = self.session.run(f"""
            select count(*) from loadTable("{self.resultDBName}", "{self.resultTBName}")
                where tradeDate between {startDate} and {endDate} group by symbol
        """)
        return symbolDF["symbol"].tolist()

    def getSignalList(self) -> List[str]:
        """
        获取当前结果数据库中信号列表
        """
        startDate = pd.Timestamp(self.startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(self.endDate).strftime("%Y.%m.%d")
        signalDF = self.session.run(f"""
                    select count(*) from loadTable("{self.resultDBName}", "{self.resultTBName}")
                        where tradeDate between {startDate} and {endDate} group by signal
                """)
        return signalDF["signal"].tolist()


class Stats(Result):    # for SignalPlot
    def __init__(self, session: ddb.session):
        super().__init__(session)

    # @lru_cache(maxsize=128)
    def getData_givenPeriodAndSignal(self, callBackPeriod: int, signalStr: str, afterStatDays: List[int]) -> Dict[str, pd.DataFrame]:
        """
        获取指定标的指定回测周期的数据
        """
        startDate = pd.Timestamp(self.startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(self.endDate).strftime("%Y.%m.%d")
        resultDict = self.session.run(f"""
        startDate = {startDate}
        endDate = {endDate}
        callBackPeriod = {callBackPeriod}
        signalStr = "{signalStr}"
        resultDB = "dfs://factorSignal"
        resultTB = "pt"
        afterStatDays = {afterStatDays}
        resultDict = dict(SYMBOL, ANY);
        
        // 最新时点数据
        posIndicator0 = ["retAvgPos","upRatePos","downRatePos"]
        negIndicator0 = ["retAvgNeg","upRateNeg","downRateNeg"]
        posIndicatorList0 = array(STRING, 0);
        negIndicatorList0 = array(STRING, 0);
        for (day in afterStatDays){{
            posIndicatorList0.append!(posIndicator0+string(day));
            negIndicatorList0.append!(negIndicator0+string(day));
        }}
        pointIndicatorList = posIndicatorList0.copy().append!(negIndicatorList0)
        pointPt = select value from (select last(value) as value from loadTable(resultDB, resultTB)
            where (tradeDate between startDate and endDate) and signal==signalStr and period == callBackPeriod and indicator in pointIndicatorList
            group by symbol, indicator) pivot by symbol, indicator
        resultDict["point"] = pointPt;
        undef(`pointPt)
        clearAllCache();
            
        // 历史序列数据
        posIndicatorList1 = ["posNum", "consUp1DRatePos", "consUp2DRatePos", "consUp3DRatePos",
                            "consDown1DRatePos", "consDown2DRatePos", "consDown3DRatePos"]
        negIndicatorList1 = ["negNum", "consUp1DRateNeg", "consUp2DRateNeg", "consUp3DRateNeg",
                            "consDown1DRateNeg", "consDown2DRateNeg", "consDown3DRateNeg"]
        seriesIndicatorList = posIndicatorList1.copy().append!(negIndicatorList1)
        seriesPt = select value from (select toArray(value) as value from loadTable(resultDB, resultTB) 
            where (tradeDate between startDate and endDate) and signal==signalStr and period == callBackPeriod and indicator in seriesIndicatorList
            group by symbol, indicator) pivot by symbol, indicator
        resultDict["series"] = seriesPt;
        undef(`seriesPt)
        clearAllCache(); // 可能会爆内存 -> 需要及时释放
        resultDict
        """)
        return resultDict

    # @lru_cache(maxsize=128)
    def getData_givenPeriodAndSymbol(self, callBackPeriod: int, symbolStr: str, afterStatDays: List[int]) -> Dict[str, pd.DataFrame]:
        """
        获取指定标的指定回测周期的数据
        """
        startDate = pd.Timestamp(self.startDate).strftime("%Y.%m.%d")
        endDate = pd.Timestamp(self.endDate).strftime("%Y.%m.%d")
        resultDict = self.session.run(f"""
        startDate = {startDate}
        endDate = {endDate}
        callBackPeriod = {callBackPeriod}
        symbolStr = "{symbolStr}"
        resultDB = "dfs://factorSignal"
        resultTB = "pt"
        afterStatDays = {afterStatDays}
        resultDict = dict(SYMBOL, ANY);

        // 最新时点数据
        posIndicator0 = ["retAvgPos","upRatePos","downRatePos"]
        negIndicator0 = ["retAvgNeg","upRateNeg","downRateNeg"]
        posIndicatorList0 = array(STRING, 0);
        negIndicatorList0 = array(STRING, 0);
        for (day in afterStatDays){{
            posIndicatorList0.append!(posIndicator0+string(day));
            negIndicatorList0.append!(negIndicator0+string(day));
        }}
        pointIndicatorList = posIndicatorList0.copy().append!(negIndicatorList0)
        pointPt = select value from (select last(value) as value from loadTable(resultDB, resultTB)
            where (tradeDate between startDate and endDate) and symbol==symbolStr and period == callBackPeriod and indicator in pointIndicatorList
            group by signal, indicator) pivot by signal, indicator
        resultDict["point"] = pointPt;
        undef(`pointPt)
        clearAllCache();

        // 历史序列数据
        posIndicatorList1 = ["posNum", "consUp1DRatePos", "consUp2DRatePos", "consUp3DRatePos",
                            "consDown1DRatePos", "consDown2DRatePos", "consDown3DRatePos"]
        negIndicatorList1 = ["negNum", "consUp1DRateNeg", "consUp2DRateNeg", "consUp3DRateNeg",
                            "consDown1DRateNeg", "consDown2DRateNeg", "consDown3DRateNeg"]
        seriesIndicatorList = posIndicatorList1.copy().append!(negIndicatorList1)
        seriesPt = select value from (select toArray(value) as value from loadTable(resultDB, resultTB) 
            where (tradeDate between startDate and endDate) and symbol==symbolStr and period == callBackPeriod and indicator in seriesIndicatorList
            group by signal, indicator) pivot by signal, indicator
        resultDict["series"] = seriesPt;
        undef(`seriesPt)
        clearAllCache(); // 可能会爆内存 -> 需要及时释放
        resultDict
        """)
        return resultDict

    def Plot_(self, panelName: str) -> None:
        """
        同一信号所有品种横向比较可视化
        """
        callBackPeriod = st.selectbox(
            label="请输入callBackPeriod长度",
            options=(i for i in self.callBackDays)
        )
        afterStatDays = self.afterStatDays[self.callBackDays.index(callBackPeriod)]
        if panelName == "A":
            st.title("Panel-A: Given callBackPeriod & Signal -> Flexible Symbol")
            signalList = self.getSignalList()
            signalStr = st.selectbox(
                label="请输入信号名称",
                options=(i for i in signalList)
            )
            Dict = self.getData_givenPeriodAndSignal(callBackPeriod=callBackPeriod, signalStr=signalStr,
                                                     afterStatDays=afterStatDays)
            indexName = "symbol"

        else:
            st.title("Panel-B: Given callBackPeriod & Symbol -> Flexible Signal")
            symbolList = self.getSymbolList()
            symbolStr = st.selectbox(
                label="请输入标的名称",
                options=(i for i in symbolList)
            )
            Dict = self.getData_givenPeriodAndSymbol(callBackPeriod=callBackPeriod, symbolStr=symbolStr,
                                                     afterStatDays=afterStatDays)
            indexName = "signal"
        pointData = Dict["point"]
        seriesData = Dict["series"]

        # 创建一个用于显示的DataFrame
        display_df = pointData.copy()

        # 为每个指标添加历史序列列（用于显示缩略图）
        for col in seriesData.columns:
            if col != seriesData.columns[0]:  # 跳过symbol列
                # 将历史序列数据添加到对应的列
                display_df[f"{col}_history"] = seriesData[col]

        # 配置列显示
        column_config = {
            display_df.columns[0]: st.column_config.TextColumn(indexName)
        }

        # 为原始数值列和缩略图列配置显示
        for col in pointData.columns:
            if col != pointData.columns[0]:
                column_config[col] = st.column_config.NumberColumn(
                    f"{col}",
                    format="%.4f"  # 可以根据需要调整小数位数
                )

        # 为历史序列列配置缩略图显示
        for col in seriesData.columns:
            if col != seriesData.columns[0]:
                history_col = f"{col}_history"
                if history_col in display_df.columns:
                    column_config[history_col] = st.column_config.LineChartColumn(
                        f"{col}",
                        width="medium",
                        help=f"{col}'s series"
                    )

        # 显示表格
        st.dataframe(
            display_df,
            column_config=column_config,
            hide_index=True,
            width='stretch',
            height=500
        )
