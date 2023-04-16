proc import datafile='/home/cityuhk/hanyan9/BIR/file_1.csv' OUT=work.p1 DBMS=csv 
		REPLACE;
	getnames=YES;
run;

proc import datafile='/home/cityuhk/hanyan9/BIR/file_2.csv' OUT=work.p2 DBMS=csv 
		REPLACE;
	getnames=YES;
run;

proc import datafile='/home/cityuhk/hanyan9/BIR/file_3.csv' OUT=work.p3 DBMS=csv 
		REPLACE;
	getnames=YES;
run;

proc import datafile='/home/cityuhk/hanyan9/BIR/file_4.csv' OUT=work.p4 DBMS=csv 
		REPLACE;
	getnames=YES;
run;

data merged_data;
  set p1 p2 p3 p4;
run;

proc sort data=merged_data out=merged_data;
    by permno datadate;
run;

data date_series;
   do date = '01JAN1997'd to '31DEC2021'd by 1;
      output;
   end;
   format date date9.;
run;

proc sql;
	create table vix as select a.*, b.vix from date_series a left join cboe.cboe b on 
		a.date=b.date order by a.date;
quit;

data vix(keep=date vix);
   set vix;
   retain last_vix time_temp;
   if not missing(vix) then do;
       last_vix = vix;
       time_temp = time; 
   end;
   else vix = last_vix;
   time = time_temp; 
   drop time_temp; 
run;

proc expand data=merged_data out=merged_data method=none;
  by permno;
  convert bwb_beta=lag1m_bwb_beta / transform=(lag 1);
  convert datadate=lag1m_datadate / transform=(lag 1);
  convert bwb_beta=lag1q_bwb_beta / transform=(lag 3);
  convert datadate=lag1q_datadate / transform=(lag 3);
run;

proc sql;
	create table merged_data as select distinct a.*, b.vix from merged_data a left join vix b on 
		a.datadate=b.date order by a.permno, a.datadate;
    create table merged_data as select distinct a.*, b.vix as lag1m_vix from merged_data a left join vix b on 
		a.lag1m_datadate=b.date order by a.permno, a.datadate;
	create table merged_data as select distinct a.*, b.vix as lag1q_vix from merged_data a left join vix b on 
		a.lag1q_datadate=b.date order by a.permno, a.datadate;	
run;


data merged_data;
  set merged_data;
  lead_datadate = intnx('month',datadate,1,'E');
  format lead_datadate date10.;
run;


proc sql;
	create table comp as select a.gvkey, a.datadate, a.fyearq, a.fqtr, a.tic, a.cusip, 
	a.indfmt, a.datafmt, a.popsrc, a.consol, a.conm, a.atq, a.xrdq, a.oibdpq, a.xintq, a.txtq,
	a.capxy, a.ibq, a.dpq, a.saleq, a.dlttq, a.cheq, a.dlcq, a.oancfy, a.sppey,
	a.txditcq, a.ltq, a.pstkq, a.prccq, a.cshoq, a.ceqq, a.apq, a.cogsq, a.ppentq, a.dvy, a.ppegtq
	from comp.fundq a where a.indfmt="INDL" 
	and a.consol="C" and a.popsrc="D" and a.datafmt="STD" and 1997<=a.fyearq<=2019 order by a.gvkey, datadate;
run;

proc expand data=comp out=comp method=none;
  by gvkey;
  convert ppentq=lag_ppentq / transform=(lag 1);
  convert atq=lag_atq / transform=(lag 1);
  convert oancfy=lag_oancfy / transform=(lag 1);
  convert oibdpq=lag_oibdpq / transform=(lag 1);
  convert xintq=lag_xintq / transform=(lag 1);
  convert txtq=lag_txtq / transform=(lag 1);
  convert dvy=lag_dvy/ transform=(lag 1);
  convert ibq=lag_ibq/ transform=(lag 1);
  convert dpq=lag_dpq/ transform=(lag 1);
  convert xrdq=lag_xrdq/ transform=(lag 1);
  convert ltq=lag_ltq/ transform=(lag 1);
  convert prccq=lag_prccq/ transform=(lag 1);
  convert cshoq=lag_cshoq/ transform=(lag 1);
  convert pstkq=lag_pstkq/ transform=(lag 1);
  convert txditcq=lag_txditcq/ transform=(lag 1);
  convert capxy=lag_capxy/ transform=(lag 1);
  convert sppey=lag_sppey/ transform=(lag 1);
run;

data comp;
	set comp;
    if fqtr=1 then net_capxy = capxy; else net_capxy = capxy-lag_capxy;
    if fqtr=1 then net_sppey = sppey; else net_sppey = sppey-lag_sppey;
	invest1 = (ppentq-lag_ppentq)/lag_atq*100;  
	invest2 = net_capxy/lag_atq*100;
	invest3 = (atq-lag_atq)/lag_atq*100;
	invest4 = net_capxy/lag_ppentq*100;
	at = log(lag_atq);                            
	q=sum(lag_ltq, lag_prccq*lag_cshoq, max(lag_pstkq, 0), -max(lag_txditcq, 0))/lag_atq;
	if lag_xrdq=. then lag_xrdq=0;
	if not missing(oancfy) then
        cash_flow1 = lag_oancfy/lag_atq;
    else
        cash_flow1 = (lag_oibdpq - lag_xintq - lag_txtq - lag_dvy) / lag_atq;
    cash_flow2=(lag_ibq+lag_dpq+lag_xrdq)/lag_atq;                    
	cash_flow3=(lag_ibq+lag_dpq)/lag_atq;
run;

proc sql;
create table gvkey_list as select distinct gvkey from comp.fundq;
run;

%INDCLASS (INSET=gvkey_list, OUTSET=gvkey_ind, FFIND=48, 
BEGDATE=01jan1985, ENDDATE=31dec2021, FREQ=month);

proc sql;
create table comp as select a.*,b.sich,b.FFI48,
floor(b.sich/100) as sic2, floor(b.sich/10) as sic3, b.sich as sic4
from comp a left join gvkey_ind b
on a.gvkey=b.gvkey and year(month_date)=year(a.datadate) and month(month_date)=month(a.datadate);
run;


proc sql;
    create table link as select gvkey, liid as iid, lpermno as permno, linktype, linkprim, 
    linkdt, linkenddt from crsp.ccmxpf_linktable where substr(linktype,1,1)='L' and
    (year(linkdt)<=2019 or linkdt = .B) and( year(linkenddt)>=1997 or linkenddt = .E)
    and (linkprim ='C' or linkprim='P');
    create table comp_link as select * from link as a, comp as b where a.gvkey = b.gvkey 
    and (linkdt <= b.datadate or linkdt = .B) and (b.datadate <= linkenddt or linkenddt = .E);
run;

proc sql;
  create table wrds_data as select a.*, b.*, (b.bwb_beta*b.vix) as beta_vix, (b.lag1m_bwb_beta*b.lag1m_vix) as lag1m_beta_vix,
    (b.lag1q_bwb_beta*b.lag1q_vix) as lag1q_beta_vix from comp_link as a
  left join merged_data as b on a.permno=b.permno and b.datadate<=a.datadate<b.lead_datadate;
quit;


*calculate 1-R^2;
proc sql;
    create table msf_link as select a.permno, a.date, a.prc, a.ret, a.shrout, (abs(a.prc)*a.shrout) as size, b.gvkey
    from crspa.dsf a, link b where 1997<=year(a.date)<=2019 and a.permno = b.permno 
    and (b.linkdt <= a.date or b.linkdt = .B) and (a.date <= b.linkenddt or b.linkenddt = .E);
    create table msf as select a.*, floor(b.sich/10) as sic3 from msf_link a left join gvkey_ind b 
    on a.gvkey=b.gvkey and year(month_date)=year(a.date) and month(month_date)=month(a.date);
    create table msf as select a.*, sum(a.size) as ind_size from msf a where sic3 is not missing group by sic3, date;
    create table msf as select a.*, size/ind_size*ret as vwret from msf a ;
    create table msf as select a.*, sum(vwret) as ind_ret , count(*) as num from msf a group by sic3, date;
run;

proc sql;
	create table temp as select a.permno, a.datadate, b.date, b.ind_ret, b.ret,
	    c.mktrf+c.rf as mkt from wrds_data a, msf b, ff.factors_daily c 
		where a.permno=b.permno and -12<=INTCK('month', b.date, a.datadate)<=-1 and b.num>=30 and 
		b.date=c.date and b.ret>=-1 order by a.permno, a.datadate, b.date desc;
quit;

proc reg data=temp outest=temp2(keep=permno datadate _RSQ_ rename=(_RSQ_=r2)) edf noprint;
	by permno datadate;
	model ret=mkt ind_ret;
run;


proc sql;
	create table wrds_data as select a.*, (1-b.r2) as info from wrds_data a left join temp2 b on 
		a.permno=b.permno and a.datadate=b.datadate order by a.permno, a.datadate;
quit;



proc expand data=wrds_data out=wrds_data method=none;
  by permno;
  convert vix=lag1m_bwb_beta / transform=(lag 1);
  convert bwb_beta=lag1q_bwb_beta / transform=(lag 3);
run;


data reg;
	set wrds_data;
	keep permno gvkey sic2 sic3 sic4 datadate fyearq fqtr invest1 invest2 invest3 invest4 
	q info vix at cash_flow1 cash_flow2 cash_flow3 bwb_beta lag1m_bwb_beta lag1q_bwb_beta beta_vix lag1m_beta_vix lag1q_beta_vix;
    retain permno datadate;
    *if sic3=. then delete;
    *if bwb_beta=. then delete;
run;

%WINSORIZE(INSET=reg, OUTSET=reg, SORTVAR=datadate, 
VARS=invest1 invest2 invest3 invest4 q vix at cash_flow1 cash_flow2 cash_flow3, PERC1=1, TRIM=0);

proc sort data=reg out=reg; by permno datadate;
run;

proc export data=reg 
		outfile='/home/cityuhk/hanyan9/BIR/reg4.xlsx' dbms=xlsx
	    replace label;
run;





data a;
   set optionm.zerocd;
   by date;
   big = (days>=30);
   small = (days<=30);
run;

proc sort data=a out=a;
   by date days;
run;

data day30(keep=date days rate);
    set a;
    by date;
    if days = 30;
run;

proc sql;
  create table daysnot30 as select * from a
  where date not in (select distinct date from a where days=30);
quit;

proc expand data=daysnot30 out=daysnot30 (drop=TIME)  method=none; 
      by date;
	  convert small=lead_small / transform=(lead 1);
	  convert small=lag_small / transform=(lag 1);
run;

data daysnot30;
   set daysnot30;
   if lag_small=. then lag_small=1;
run;


data daysnot30;
    set daysnot30;
    by date;
    where (small=1 and big=0 and lag_small=1 and lead_small=0) or (small=0 and big=1 and lag_small=1 and lead_small=0);
run;

proc sort data=daysnot30 out=daysnot30;
   by date days;
run;

data temp;
    set daysnot30;
    by date;
    if first.date;
    days = 30;
    rate =.;
run;

data temp2 (keep= date days rate);
   set daysnot30 temp;
run;

proc sort data=temp2 out=temp2;
   by date days;
run;

proc expand data=temp2 out=temp2(keep=date days linear rename=(linear=rate));
   by date;
   convert rate=linear / method=join;
   id days;
run;

data rf;
   set day30 temp2;
   where days=30;
   format date date9.
run;

proc sort data=rf out=rf;
   by date days;
run;

/*risk neutral moment for all stocks, not necessarily sp500*/
%let day_choose = 30;

/*volatility_surface data*/
%macro data_read;
  %do i=1996 %to 2021;
    proc sql;
      create table vs_rnm_&i. as 
      select a.secid, a.date, a.days, a.impl_volatility, a.impl_strike, a.impl_premium, a.cp_flag, c.close 
      from optionm.vsurfd&i. as a, optionm.secprd&i. as c
      where a.date = c.date and a.secid = c.secid and a.impl_volatility>0 
      and a.impl_strike>0 and c.close>0 and a.days=&day_choose.
      and ((impl_strike>=close and cp_flag='C') or (impl_strike<close and cp_flag='P'));
    quit;
  %end;
%mend data_read;
%data_read;

data vs;set vs_rnm_:;run;


/*merge data*/
proc sort data=vs;
  by date days secid impl_strike;
run;

data vs;
  merge vs(in=a) rf(in=b);
  by date days;
  if a and b;
run;

/*two option prices, one is impl_premium, the other is the BS price without dividend*/
data vs;
  set vs;
  forward=close*exp(rf*days/365/100);
  d1=(log(forward/impl_strike)+0.5*days/365*impl_volatility**2)/(impl_volatility*sqrt(days/365));
  d2=(log(forward/impl_strike)-0.5*days/365*impl_volatility**2)/(impl_volatility*sqrt(days/365));
  if cp_flag='C' 
  	then opr=forward*exp(-rf*days/365/100)*cdf('NORMAL',d1)-impl_strike*exp(-rf*days/365/100)*cdf('NORMAL',d2);
  if cp_flag='P' 
  	then opr=impl_strike*exp(-rf*days/365/100)*cdf('NORMAL',-d2)-forward*exp(-rf*days/365/100)*cdf('NORMAL',-d1);
  drop d1 d2 forward days impl_volatility cp_flag rf;
run;
/*calculate v w x*/
proc sort data=vs;
  by secid date impl_strike;
run;
proc expand data=vs out=vs;
  convert impl_strike = k_bef /transformout=(lag 1);
  convert impl_strike = k_aft /transformout=(lead 1);
  by secid date;
run;
data vs;
  set vs;
  if k_bef=. and k_aft=. then delete;
  if opr<=0 then delete;
  if impl_premium<=0 then delete;
  drop TIME;
run;
/*can change impl_premium to opr, should be similar*/
data vs;
  set vs;
  delta_k=(k_aft-k_bef)/2;
  if k_bef=. then delta_k=k_aft-impl_strike;
  if k_aft=. then delta_k=impl_strike-k_bef;
  v_part=2*(1-log(impl_strike/close))/(impl_strike**2)*impl_premium*delta_k;
  w_part=(6*log(impl_strike/close)-3*(log(impl_strike/close))**2)/(impl_strike**2)*impl_premium*delta_k;
  x_part=(12*(log(impl_strike/close))**2-4*(log(impl_strike/close))**3)/(impl_strike**2)*impl_premium*delta_k;
run;
proc summary data=vs;
  by secid date;
  var v_part w_part x_part;
  output out=rnm(drop=_type_ _freq_) sum=v w x;
run;
/*calclulate risk neutral moment*/
proc sort data=rnm;
  by date secid;
run;
proc sort data=rf;
  by date;
run;
data rnm;
  merge rnm(in=a) rf(in=b);
  by date;
  if a and b;
run;
data rnm;
  set rnm;
  miu=exp(rf*days/365/100)-1-exp(rf*days/365/100)*(v/2+w/6+x/24);
  rnv=exp(rf*days/365/100)*v-miu**2;
  rns=(exp(rf*days/365/100)*w-3*miu*v*exp(rf*days/365/100)+2*miu**3)/(rnv**1.5);
  keep secid date rnv rns rnk;
run;


/*rns in month-end*/
data rnm;
  set rnm;
  date_c=put(date,yymmn6.);
run;
proc sort data=rnm;
  by secid date_c date;
run;
data rnm_end;
  set rnm;
  by secid date_c date;
  if last.date_c;
run;
proc means data=rnm_end N NMISS MEAN STDDEV MIN P1 P25 P50 P75 P99 MAX;
run;

/*add permno to risk neutral moments*/
proc sql;
  create table rnm_permno as
  select a.*,b.permno
  from rnm_end as a, wrdsapps.opcrsphist as b 
  where a.secid = b.secid and a.date >= b.sdate and a.date <= b.edate and b.score=1; /*link score constraint*/
quit;

/*check duplicates, no duplicates*/
proc sort data=rnm_permno out=bir.rnm nodupkey;
  by permno date;
run;

proc datasets;delete vs_rnm_:;run;
