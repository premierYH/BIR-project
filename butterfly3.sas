proc import datafile='/home/cityuhk/hanyan9/BIR/bir.csv' OUT=work.bir DBMS=csv 
		REPLACE;
	getnames=YES;
run;

proc sort data=work.bir out=bir;
    by permno corrdate;
run;

proc expand data=bir out=bir method=none;
  by permno;
  convert bir_it=lag_bir / transform=(lag 1);
run;

data bir;
    set bir;
    lead_corrdate = intnx('month',corrdate,1,'E');
    format lead_corrdate date10.;
    year = year(corrdate);
    month = month(corrdate);
run;

data time_series;
  format date date9.;
  do date = '01JAN1996'd to '31DEC2021'd;
    output;
  end;
run;

proc import datafile='/home/cityuhk/hanyan9/BIR/DGS1.csv' OUT=work.dgs1 DBMS=csv 
		REPLACE;
	getnames=YES;
run;

proc import datafile='/home/cityuhk/hanyan9/BIR/DGS10.csv' OUT=work.dgs10 DBMS=csv 
		REPLACE;
	getnames=YES;
run;

*"yield" contains 10 year treasury bond yield and 1 year bond yield;
proc sql;
create table yield as select a.date, b.dgs1, c.dgs10 from time_series a left join dgs1 b 
on a.date=b.date left join dgs10 c on a.date=c.date;
quit;

proc expand data=yield out=yield(drop=dgs10 dgs1) method=none;
  id date;
  convert dgs10=yield10 / method=stepdown;
  convert dgs1=yield1 / method=stepdown;
run;


proc sql;
	create table comp as select a.gvkey, a.datadate, a.fyearq, a.fqtr, a.tic, a.cusip, 
	a.indfmt, a.datafmt, a.popsrc, a.consol, a.conm, a.atq, a.actq, a.lctq, a.xrdq, 
	a.capxy, a.ibq, a.dpq, a.saleq, a.dlttq, a.cheq, a.dlcq, a.seqq, a.dvpq, a.aqcy, a.rectq,
	a.txditcq, a.ltq, a.pstkq, a.prccq, a.cshoq, a.ceqq, a.apq, a.cogsq, a.ppentq, a.dvy,
	a.dd1q, a.ibadjq, a.cshprq, a.ppegtq, a.cogsq, (b.yield10-b.yield1) as term_spread 
	from comp.fundq a left join yield b on a.datadate = b.date where a.indfmt="INDL" 
	and a.consol="C" and a.popsrc="D" and a.datafmt="STD" and 1985<=a.fyearq<=2022 order by a.gvkey, datadate;
run;

proc sort data=comp out=comp;
    by gvkey datadate;
run;

data comp;
	set comp;
	by gvkey datadate;
	if first.datadate;
	lag_datadate = intnx('month',datadate,-3,'E');
	format lag_datadate date10.;
	*calculate number of actual days in the quarter;
	act_days = datadate - lag_datadate;
run;

proc expand data=comp out=comp (drop=TIME) method=none;
	by gvkey;
	convert datadate=lag_datadate / transform=(lag 1);
	convert atq=lag_atq / transform=(lag 1);
	convert ibadjq=lag_ibadjq /  transform=(lag 1);
	convert rectq=lag_rectq /  transform=(lag 1);
run;

data comp;
	set comp;
	cash_hold=cheq/lag_atq;                      /*cash_t/asset_t-1*/
	log_at=log(atq);                             /*total asset*/
	leverage=(dlttq+dlcq)/(dlttq+dlcq+seqq);     /*leverage*/
	mb=(atq-ceqq+prccq*cshoq)/atq;               /*market to book ratio*/
	cash_flow=(ibq+dpq)/atq;                     /*cash flow/net assets*/
	nwc=(actq-lctq-cheq)/atq;                    /*NWC/net assets*/
	inv=capxy/atq;                               /*capital intensity:Capex/net assets*/
	div_dummy=(dvpq>0);                          /*dividend dummy*/
    aqc = aqcy/atq;                              /*acquisition activity */
    /*Trade credit: accounts Receivable is the logarithm of accounts receivable to sales.*/
    tc1 = log(rectq/saleq);
    /*Trade credit: change in Accounts Receivable is the logarithm of one plus the ratio of accounts receivables to its own lagged value.*/
    tc2 = log(1+rectq/lag_rectq);
	/*Trade credit: logarithm of the firm’s accounts payable to sales ratio*/
	tc3 = log(apq/saleq);
	/*Trade credit: (Trade receivable/Sales) × Number of actual days in the quarter -  (Trade payables/Cost of goods sold) × Number of actual days in the quarter*/
	*rec_days = rectq/saleq*act_days;
	*pay_days = apq/cogsq*act_days;
	*tc4 = rec_days-pay_days;
	*gm = (saleq-cogsq)/saleq;                    /*gross margin*/
	*turnover = saleq/atq;                        /*asset turnover*/
	*fc = (ppentq-dlttq)/ppentq;                  /*free collatoral*/
	*debt = dlcq/atq;                             /*short-term debt-to-asset ratio*/
	*ca = cheq/atq;                               /*cash to asset*/
	/*RSI(relationship-specific investment): ratio of R&D expenditure to assets, namely rd3*/
	roa = ibq/atq;                               /*roa*/
    q=sum(ltq, prccq*cshoq, max(pstkq, 0), -max(txditcq, 0))/atq;
    log_sales = log(saleq);
    tg = ppentq/atq;                             /*tangibility Ratio is the ratio of net property, plant, and equipment to assets.*/
    div = dvy/lag_atq; 
	stdebt1 = dlcq/(dlcq+dlttq);                 /*short-term debt rate measure 1*/
	stdebt2 = dd1q/(dlcq+dlttq);                 /*short-term debt rate measure 2*/  
	ltdebt = dlttq/(dlcq+dlttq);                 /*long-term debt rate*/
	mkt_cap = prccq*cshoq;                       /*market capitalization*/
	ab_earn = (ibadjq-lag_ibadjq)/(prccq*cshprq);/*abnormal earnings*/
    am = ppegtq**2/(dpq*atq)+actq**2/(cogsq*atq);  /*asset maturity*/
run;

proc sort data=comp out=comp; by datadate;
run;

proc rank data=comp out=temp; var mkt_cap; by datadate; ranks myrank;
run;

/*calculate the size (percent of NYSE firms that have the same or smaller market capitalization)*/
proc sql;
create table comp as select *, count(mkt_cap) as num from comp group by datadate;
create table comp as select a.*, b.myrank, (b.myrank/a.num) as size from comp a left join temp b 
on a.datadate=b.datadate and a.gvkey = b.gvkey;
run;

proc sort data=comp out=comp; by gvkey datadate;
run;

proc expand data=comp out=comp (drop=TIME) method=none;
	by GVKEY;
	convert cash_flow=lag1_cash_flow / transform=(lag 1);
	convert cash_flow=lag2_cash_flow / transform=(lag 2);
	convert cash_flow=lag3_cash_flow / transform=(lag 3);
	convert cash_flow=lag4_cash_flow / transform=(lag 4);
	convert cash_flow=lag5_cash_flow / transform=(lag 5);
	convert cash_flow=lag6_cash_flow / transform=(lag 6);
	convert cash_flow=lag7_cash_flow / transform=(lag 7);
	convert cash_flow=lag8_cash_flow / transform=(lag 8);
	convert cash_flow=lag9_cash_flow / transform=(lag 9);
	convert cash_flow=lag10_cash_flow / transform=(lag 10);
	convert saleq=lag4_saleq / transform=(lag 4);
run;

data comp;
	set comp;
	std_cash_flow=std(lag1_cash_flow, lag2_cash_flow, lag3_cash_flow, 
		lag4_cash_flow, lag5_cash_flow, lag6_cash_flow, lag7_cash_flow, 
		lag8_cash_flow, lag9_cash_flow, lag10_cash_flow);
run;

/*merge with time-varying sic*/
proc sql;
create table gvkey_list as select distinct gvkey from comp.fundq;
quit;

%INDCLASS (INSET=gvkey_list, OUTSET=gvkey_ind, FFIND=48, 
BEGDATE=01jan1985, ENDDATE=31dec2021, FREQ=month);

proc sql;
create table comp as select a.*,b.sich,b.FFI48,
floor(b.sich/100) as sic2, floor(b.sich/10) as sic, b.sich as sic4
from comp a left join gvkey_ind b
on a.gvkey=b.gvkey and year(month_date)=year(a.datadate) and month(month_date)=month(a.datadate);
quit;

*calculate industry_sigma;
proc sql;
	create table comp as select a.*, median(a.std_cash_flow) as industry_sigma from 
		comp a group by sic2, fyearq order by sic2, fyearq;
run;

proc sort data=comp out=comp;
    by gvkey datadate;
run;

*gvkey-permno linkage;
proc sql;
    create table link as select gvkey, liid as iid, lpermno as permno, linktype, linkprim, 
    linkdt, linkenddt from crsp.ccmxpf_linktable where substr(linktype,1,1)='L' and
    (year(linkdt)<=2022 or linkdt = .B) and( year(linkenddt)>=1995 or linkenddt = .E)
    and (linkprim ='C' or linkprim='P');
    create table comp_link as select * from link as a, comp as b where a.gvkey = b.gvkey 
    and (linkdt <= b.datadate or linkdt = .B) and (b.datadate <= linkenddt or linkenddt = .E);
run;

*get sp500 constituents;
proc sql;
    create table sp500_ccm as select a.*, b.* from crsp.msp500list a left join comp_link b on a.permno=b.permno
    where a.start<=b.datadate<a.ending;
run;

proc sort data=sp500_ccm out=sp500_ccm;
    by gvkey datadate;
run;

data sp500_ccm;
	set sp500_ccm;
	by gvkey datadate;
	if first.datadate;
run;

proc sql;
    create table m1 as select a.*,b.* from sp500_ccm a left join bir b on a.permno=b.permno 
    where b.corrdate<=a.datadate<b.lead_corrdate ;
run;

*extrct daily ret for calculating asset volatility;
proc sql;
    create table daily_rets as select a.datadate,a.permno, b.date, b.ret from m1 a, crspa.dsf b
		where a.permno=b.permno and -2<=INTCK('month', a.datadate, b.date)<=0 
		and b.ret>=-1 order by a.permno, a.datadate desc;
	create table daily_rets as select a.*, std(a.ret) as std_ret from 
		daily_rets a group by a.permno, a.datadate order by a.permno, a.datadate, a.date;
run;

proc sort data=daily_rets nodupkey out=daily_rets;
by permno datadate;
run;

proc sql;
	create table m2 as select a.*, b.std_ret from m1 a left join daily_rets b on a.permno=b.permno where
	    a.datadate=b.datadate;
run;

data m2;
    set m2;
    avol = std_ret*cshoq*prccq/(atq+cshoq*prccq-ceqq); /*asset volatility*/
run;   

proc sort data=m2 nodupkey out=m2; by permno datadate descending corrdate;
run;

*Determine whether the data for each quarter is missing or not;
proc sql;
    create table m3 as select *, sum(xrdq) as sum_xrdq,
    sum(case when fqtr=1 and xrdq=. then 1 else 0 end) as empty_q1,
    sum(case when fqtr=2 and xrdq=. then 1 else 0 end) as empty_q2,
    sum(case when fqtr=3 and xrdq=. then 1 else 0 end) as empty_q3,
    sum(case when fqtr=4 and xrdq=. then 1 else 0 end) as empty_q4
    from m2 group by permno, fyearq;
quit;

proc sort data=m3 out=m3; by permno fyearq fqtr;
run;

*Check if the values of the first three quarters are missing and the value of the fourth quarter is not missing. 
If so, assign the value of the fourth quarter divided by 4 to the first three quarters.;
data m3;
    set m3;
    if empty_q1=1 and empty_q2=1 and empty_q3=1 and empty_q4=0 then xrdq = sum_xrdq/4;
	*R&D/sales;
	rd1=xrdq/saleq;
	*R&D/lag4_sales;
	rd2=xrdq/lag4_saleq;
	*R&D/assets;
    rd3=xrdq/atq;
run;

data m3;
	set m3;
	keep permno gvkey sic sic2 sic4 datadate corrdate lead_corrdate fyearq fqtr log_at cash_hold
	leverage mb cash_flow nwc inv rd1 rd2 rd3 bir_it lag_bir div_dummy industry_sigma aqc tc1 tc2 tc3 
	roa q log_sales tg div stdebt1 stdebt2 ltdebt size mkt_cap ab_earn am avol term_spread;
    retain permno datadate corrdate lead_corrdate;
    *Any remaining missing values of R&D or total debt are replaced with zero;
	if rd1=. then rd1=0;
	if rd2=. then rd2=0;
	if rd3=. then rd3=0;
	*Remove any observations with zero, negative, or missing total assets (item ATQ);
	if log_at=. then delete; 
run;

%WINSORIZE(INSET=m3, OUTSET=reg, SORTVAR=datadate, 
VARS=log_at cash_hold leverage mb cash_flow nwc inv rd1 rd2 rd3 aqc tc1 tc2 tc3
 roa q log_sales tg div stdebt1 stdebt2 ltdebt ab_earn am avol term_spread, 
	PERC1=1, TRIM=0);

proc means data = reg N MEAN Q1 MEDIAN Q3 STDDEV MAX MIN;
var log_at cash_hold leverage mb cash_flow nwc inv rd1 rd2 rd3 bir_it lag_bir industry_sigma aqc tc1 tc2 tc3  
roa q log_sales tg div stdebt1 stdebt2 ltdebt size ab_earn am avol term_spread;
title 'Summary of Variables';
run;

proc export data=reg 
		outfile='/home/cityuhk/hanyan9/BIR/reg.xlsx' dbms=xlsx 
		replace label;
run;

proc export data=bir 
		outfile='/home/cityuhk/hanyan9/BIR/bir2.xlsx' dbms=xlsx 
		replace label;
run;


