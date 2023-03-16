proc import datafile='/home/cityuhk/hanyan9/BIR/bir.csv' OUT=work.bir DBMS=csv 
		REPLACE;
	getnames=YES;
run;

proc sort data=work.bir out=bir;
    by permno corrdate;
run;

data bir;
    set bir;
    lead_corrdate = intnx('month',corrdate,1,'E');
    format lead_corrdate date10.;
    year = year(corrdate);
    month = month(corrdate);
run;

proc sql;
	create table comp as select a.gvkey, a.datadate, a.fyearq, a.fqtr, a.tic, a.cusip, 
	a.indfmt, a.datafmt, a.popsrc, a.consol, a.conm, a.atq, a.actq, a.lctq, a.xrdq, 
	a.capxy, a.ibq, a.dpq, a.saleq, a.dlttq, a.cheq, a.dlcq, a.seqq, a.dvpq, a.prccq, 
	a.cshoq, a.ceqq from comp.fundq a, comp.company b where a.gvkey=b.gvkey 
	and a.indfmt="INDL" and a.consol="C" and a.popsrc="D" and a.datafmt="STD" 
	and 1985<=a.fyearq<=2022 order by a.gvkey, datadate;
run;

proc sort data=comp out=comp;
    by gvkey datadate;
run;

data comp;
	set comp;
	by gvkey datadate;
	if first.datadate;
run;

proc expand data=comp out=comp (drop=TIME) method=none;
	by gvkey;
	convert datadate=lag_datadate / transform=(lag 1);
	convert atq=lag_atq / transform=(lag 1);
run;

data comp;
	set comp;
	format beg end yymmdd10.;
	/*cash_t/asset_t-1*/
	cash_hold=cheq/lag_atq;
    /*size*/
	log_at=log(atq);
	
	leverage=(dlttq+dlcq)/(dlttq+dlcq+seqq);
	/*market to book asset ratio*/
	mb=(atq-ceqq+prccq*cshoq)/atq;

	/*Cash flow/net assets*/
	cash_flow=(ibq+dpq)/atq;

    /*NWC/net assets*/
	nwc=(actq-lctq-cheq)/atq;

    /*Capital intensity:Capex/net assets*/
	inv=capxy/atq;

    /*Dividend dummy*/
	div_dummy=(dvpq>0);

	beg=intnx('month', lag_datadate, 1, 'beg');
    end=intnx('month', datadate, 0, 'end');

    if lag_datadate=. then beg=intnx('month', datadate, -11, 'beg');
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


proc sql;
    create table link as select gvkey, liid as iid, lpermno as permno, linktype, linkprim, 
    linkdt, linkenddt from crsp.ccmxpf_linktable where substr(linktype,1,1)='L' and
    (year(linkdt)<=2022 or linkdt = .B) and( year(linkenddt)>=1995 or linkenddt = .E)
    and (linkprim ='C' or linkprim='P');
    create table comp_link as select * from link as a, comp as b where a.gvkey = b.gvkey 
    and (linkdt <= b.datadate or linkdt = .B) and (b.datadate <= linkenddt or linkenddt = .E);
run;


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

proc sort data=m1 nodupkey out=m2; by permno datadate descending corrdate;
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
	leverage mb cash_flow nwc inv rd1 rd2 rd3 bir_it div_dummy industry_sigma;
    retain permno datadate corrdate lead_corrdate;
    *Any remaining missing values of R&D or total debt are replaced with zero;
	if rd1=. then rd1=0;
	if rd2=. then rd2=0;
	if rd3=. then rd3=0;
	*Remove any observations with zero, negative, or missing total assets (item ATQ);
	if log_at=. then delete; 
run;

%WINSORIZE(INSET=m3, OUTSET=reg, SORTVAR=datadate, 
VARS=log_at cash_hold leverage mb cash_flow nwc inv rd1 rd2 rd3, 
	PERC1=1, TRIM=0);

proc means data = reg N MEAN Q1 MEDIAN Q3 STDDEV MAX MIN;
var log_at cash_hold leverage mb cash_flow nwc inv rd1 rd2 rd3 bir_it industry_sigma;
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
