from include.eczachly.trino_queries import execute_trino_query, run_trino_dq
from include.eczachly.aws_secret_manager import get_secret
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import pyarrow as pa
import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, DateType, DoubleType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.exceptions import NoSuchTableError
import requests
from dotenv import load_dotenv
from ast import literal_eval
import logging

logger = logging.getLogger(__name__)

load_dotenv()


tabular_credential = get_secret("TABULAR_CREDENTIAL")
POLYGON_API_KEY = literal_eval(get_secret("POLYGON_CREDENTIALS"))['AWS_SECRET_ACCESS_KEY']

schema = 'bigchungus0148'

catalog = load_catalog(
    name="",
    type="",
    uri="",
    warehouse=get_secret(""),  # Adjust secret name
    credential=tabular_credential,
)

@dag(
    description="A dag to incremental load stock activity table",
    default_args={  
        "owner": schema,
        "start_date": datetime(2025, 2, 25),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    max_active_runs=1,
    schedule_interval="0 0 * * *",
    catchup=True,
    tags=["community"],
)
def daily_raw_data_load_dag_v1():
    MAANG_STOCKS = [
                'AAPL','NVDA','MSFT','AMZN','GOOG','GOOGL','META','TSLA','AVGO','COST','NFLX','TMUS','ASML','CSCO','PLTR','AZN','LIN',
                'ISRG','PEP','ADBE','TXN','QCOM','PDD','AMD','BKNG','AMGN','INTU','ARM','APP','AMAT','HON','GILD','CMCSA','SNY','SBUX',
                'PANW','ADP','VRTX','ADI','MELI','LRCX','MU','INTC','KLAC','CRWD','ABNB','MRVL','CME','EQIX','CEG','FTNT','DASH','MDLZ',
                 'CTAS','MSTR','MAR','REGN','TEAM','ORLY','PYPL','SNPS','CDNS','WDAY','NTES','CSX','ROP','JD','ADSK','NXPI','COIN','CHTR',
                 'AEP','PCAR','CPRT','PAYX','MNST','NDAQ','KDP','HOOD','FANG','BKR','ROST','TCOM','EXC','LULU','FAST','CTSH','VRSK','GEHC',
                 'DDOG','CCEP','XEL','ARGX','AXON','ODFL','TTWO','KHC','IDXX','TTD','DXCM','EA','EBAY','ACGL','FER','MCHP','SMCI','WTW','ALNY',
                 'BIDU','TW','MPWR','UAL','CSGP','ZS','TSCO','ANSS','BNTX','FITB','FCNCA','FCNCO','LPLA','LI','ONC','ERIC','WBD','FOXA','EXPE',
                 'NTAP','ZM','FOX','CDW','FWONK','EXE','FWONA','WDC','CHKP','IBKR','TROW','HBAN','GFS','SBAC','ON','RYAAY','DKNG','NTRS','SSNC',
                 'VRSN','AFRM','NTRA','STX','VOD','CINF','TPG','BIIB','MDB','ERIE','GRAB','STLD','PODD','PTC','KSPI','RPRX','PFG','CYBR','TER',
                'NTNX','WMG','Z','CG','COO','TRMB','ZG','FFIV','DOCU','DUOL','NWS','JBHT','ULTA','FUTU','AZPN','GEN','FSLR','SMMT','ICLR',
                'NWSA','UTHR','DLTR','LNT','SOFI','ZBRA','DPZ','OKTA','ENTG','EVRG','ARCC','LOGI','FLEX','CASY','MNDY','ILMN','RIVN','INSM',
                'HOLX','PAA','SYM','BSY','SFM','ALGN','GMAB','INCY','ALAB','GLPI','MORN','MRNA','ITCI','LINE','CART','VTRS','ESLT','MBLY',
                'REG','FTAI','SAIL','POOL','BMRN','EWBC','ROKU','AUR','PPC','LAMR','JKHY','NDSN','COKE','CPB','NBIX','CHRW','RKLB','LECO',
                'LBRDK','LBRDA','AKAM','PCTY','HST','HTHT','TXRH','CRDO','MANH','WIX','WWD','SWKS','SAIA','MMYT','GGAL','TEM','LKQ','VNOM',
                'GTLB','MRNO','MEDP','CFLT','EXEL','TECH','SEIC','SRPT','TLN','AAL','PCVX','DOX','RGLD','HSIC','DSGX','WYNN','ALTR','BILI',
                'AGNC','NICE','HAS','HQY','NBIS','LCID','LSCC','EXAS','ASND','MASI','MIDD','MTSI','FYBR','RGEN','VFS','FRHC','WBA','PNFP',
                'TTAN','GDS','LNW','CHDN','ENPH','CBSH','HLNE','SATS','APA','AAON','SIRI','ASTS','SFD','DBX','MTCH','PONY','WTFC','JAZZ',
                'PARAA','TTEK','FSV','ZION','XP','NVMI','UMBF','EXLS','APPF','BRKR','CELH','PARA','DRS','RVMD','ROIV','GLBE','CZR','CVLT',
                'ONB','OLED','MDGL','MKTX','LEGN','WAY','CWST','ENSG','BECN','MAT','LLYVK','HALO','QRVO','LLYVA','BPOP','HCP','COOP','OTEX',
                'BBIO','BZ','BOKF','MKSI','VERX','STEP','CCCS','CHRD','UPST','AXSM','NXT','WING','WRD','PEGA','RMBS','CORT','UFPI','QFIN',
                'WSC','TLX','SLM','CIGI','SRAD','LFUS','CROX','GRFS','MMSI','CACC','DJT','CYTK','OLLI','BPMC','OS','CRUS','ALKS','CHX','NUVL',
                'LSTR','CRVL','CGNX','VRNA','COLB','GNTX','ETSY','AMKR','VIRT','LYFT','OPCH','LNTH','IDCC','KRYS','RRR','OZK','LANC','SPSC'
                'VLY','BCPC','ACIW','GH','FFIN','IEP','INTA','FRSH','COLM','REYN','LOPE','IONS','FCFS','NSIT','CSWI','LITE','ACT','PAGP','NOVT',
                'SLAB','TSEM','QLYS','MARA','ALGM','HWC','FRPT','PECO','URBN','ESGR','VRNS','UBSI','RELY','SAIC','AEIS','SIGI','WFRD','FELE',
                'KC','FIVE','SANM','NXST','BWIN','TENB','TIGO','AVT','BGC','CALM','EEFT','TGTX','RUSHA','RDNT','RUSHB','FROG','AVAV','SWTX',
                'FTDR','ATAT','IPAR','EXPO','VRRM','SITM','ITRI','GBDC','CVCO','CLBT','VCTR','CRSP','ROAD','BRZE','GLNG','LBTYK','LBTYB','PYCR',
                'IBOC','SNEX','LBTYA','GSHD','AKRO','RARE','IAC','PTCT','SKYW','VNET','OMAB','IESC','BANF','ICUI','SHC','KTOS','SOUN','FIZZ','VKTX','CCOI','SBRA','DORM','SMPL','ADMA','IRTC','XRAY','PLXS','RNA','NCNO','TFSL','ALVO','POWI','ACHC','ZLAB','TCBI','RXRX','IMVT','HEES','DLO','FULT','EBC','STRL','PCH','SRRK','ASO','PAYO','BHF','LIF','RIOT','CRNX','CAMT','ACLX','RYTM','NMRK','APLS','ARLP','OTTR','OSIS','BTSG','IRDM','SNRE','PRCT','FHB','PTON','MGEE','RUM','ACAD','AVPT','ZI','NWE','BLKB','AMBA','MTSR','CARG','FIBK','CATY','MRUS','PTEN','VEON','WSFS','SGRY','PTVE','SMTC','FIVN','PLMR','FA','ALKT','PENN','MGRC','WDFC','WEN','BL','CAR','VCYT','PATK','XENE','CORZ','RGTI','NFE','MEOH','DOOO','DNLI','ACVA','AMED','LAUR','LFST','ALRM','PRVA','STNE','CNXC','INDB','VICR','GT','NWL','SYNA','CAKE','CVBF','HCM','PI','SHOO','WB','FOLD','PDCO','FORM','NMIH','LUNR','QDEL','SFNC','USLM','MRX','GCMG','IPGP','TOWN','PSMT','TBBK','MLTX','HUBG','YY','WGSWW','WGS','GSAT','DIOD','PLTK','CLSK','TTMI','FFBC','IOSP','CPRX','VIAV','LIVN','VCEL','EWTX','CHEF','MRCY','ALHC','MLCO','BATRA','JJSF','BTDR','MIRM','FRME','ODD','ARWR','HSAI','IBRX','MGNI','JBLU','STRA','APPN','TWST','BATRK','DRVN','CENT','AMRX','BEAM','TMDX','VC','LION','NVCR','PRGS','SBCF','WAFD','KYMR','IREN','APLD','WSBC','MCW','PPBI','PSNY','PSNYW','BANR','PTGX','INTR','LGND','CRTO','LMAT','KLIC','TRMK','RNW','NBTB','CLOV','FLYW','CNTA','EFSC','HWKN','NEOG','IQ','CENTA','MESO','AGYS','SYBT','ENLT','POWL','COCO','CGON','CERT','TRIP','UPWK','GTX','WERN','SSRM','JANX','PWP','ACLS','ZD','NEXT','AZTA','ENVX','VSEC','TNDM','HURN','EXTR','AGIO','RPD','SLNO','NAMS','ARCB','OSW','SIMO','PGNY','HRMY','HLMN','ROCK','APGE','PSEC','ADUS','UFPT','SKWD','MYRG','CIFR','JAMF','TVTX','IDYA','BCRX','MBIN','AVDX','RUN','GOGL','ICFI','ATRC','EVCM','HIMX','MQ','PAX','ADEA','GDRX','CASH','OMCL','IRON','TRMD','LGIH','TARS','SUPN','VERA','AAPG','ULCC','SBLK','INOD','GDYN','CSGS','GLPG','IOVA','BLTE','SJW','ASTH','PLUS','PCT','SABR','LOT','OPRA','HPK','FBNC','CHCO','AMPL','PRDO','CNXN','MTTR','NTCT','PINC','WVE','IAS','UCTT','HUT','STGW','LKFN','DVAX','LUNRW','SDGR','MCRI','TWFG','ACMR','SPT','IART','PLYA','CENX','CLBK','SEZL','NWBI','XMTR','EH','MNKD','ARHS','EXPI','EVO','TFIN','INFN','SRCE','AFYA','WULF','SPNS','PZZA','DNUT','GERN','CMPO','ETNB','NN','LX','ANDE','TIGR','VITL','UPBD','ARQT','GO','FORTY','MLKN','ROOT','VRNT','PLUG','STBA','ATEC','AMPH','LZ','IMCR','CLDX','DMLP','NYAX','HDL','ATSG','TCBK','ALGT','KNSA','UDMY','KARO','NAVI','PRAX','VECO','SASR','NEO','DYN','SONO','VBTX','DXPE','MXL','MOMO','TRUP','SNDX','BUSE','PLAB','UNIT','MFIC','GRAL','LQDA','CRAI','LILA','GPCR','LILAK','DSGR','WBTN','HELE','CCB','AAOI','FTRE','DCOM','TXG','DSP','OCSL','VIR','HOPE','INMD','WABC','SGML','QCRH','NMFC','MYGN','VRDN','DAVE','CRCT','TASK','CLMT','HTZ','NRIX','NVAX','RBCAA','FLNC','DAWN','TILE','SYRE','TBLA','DGII','NTLA','CRSR','BLFS','CMPR','GIII','HLIT','FWRG','ADPT','ARVN','WLFC','IMKTA','IMKT.A','MRTN','ARDX','WINA','PLSE','CDNA','KRNT','RXST','DCBO','ECPG','CSWC','ACDC','MDXG','GABC','VSAT','PCRX','PFBC','NVEE','SAFT','IGIC','KALU','FSUN','XPEL','PENG','QNST','AUPH','ICHR','DGNX','INVA','ANIP','OPK','BLBD','HEPS','AHCO','PEBO','XNCR','OCUL','RCKT','SPRY','FOXF','INDV','COMM','BELFB','REPL','NNE','PGY','ELVN','LQDT','AMWD','BELFA','CCEC','APOG','TTGT','CBRL','BBSI','GOGO','BRKL','MRVI','OCFC','REAX','HROW','BFC','SEDG','THRM','PTLO','AMSC','PFC','BLMN','AOSL','ARRY','HSTM','AMAL','OPEN','ZYME','GYRE','AIOT','XRX','COHU','EOSE','CTBI','RDFN','QUBT','CNOB','RDWR','SBGI','SLRC','BGM','AMSF','OLPX','COLL','BASE','ESTA','BHRB','TRIN','TH','EYE','TRS','IIIV','FBYD','AVBP','PAHC','PLAY','CGBD','MSEX','FMBH','LMB','NSSC','SEAT','OCS','IPX','ABCL','PHVS','UXIN','TLRY','PDFS','PRAA','VMEO','GDEN','SNCY','ANGI','EOLS','CBLL','COGT','EVGO','BBNX','NESR','AVO','ARKO','SCSC','SION','FUFU','BJRI','UVSP','PNTG','ADTN','JBSS','ERII','HSII','ATLC','NBN','KRUS','PRTA','HTLD','STKL','HCKT','STAA','RBBN','ITRN','OSBC','HFWA','CNCK','EMBC','AVAH','CEVA','AVDL','FWRD','TCPC','NBBK','BCYC','CECO','BMBL','PRTH','WOOF','SSYS','CFB','OPT','AXGN','ASTL','HBT','ADV','FDUS','THRY','IMNM','BFST','GHRS','HCSG','CTLP','MLAB','MBWM','NRDS','GCT','MCBS','CFFN','DH','ADSE','IMOS','NTGR','WRLD','SHLS','MATW','SIBN','CRON','PSIX','EZPW','CRESY','CSIQ','TRNS','DAKT','CVAC','FCBC','SWIM','EVER','HBNC','FNKO','CAC','ULH','ATEX','JACK','ABL','TIPT','CSTL','IRMD','ATRO','HAFC','AVXL','REAL','BCAX','MGPI','ASTE','DNTH','ECX','IBCP','GOOD','PUBM','TYRA','GSBC','MBUU','INDI','EGBN','QURE','OFIX','CCAP','CAPR','FIP','ATGL','RPAY','SANA','TMC','FSBC','RXT','SPTN','SMBC','CTKB','CGNT','PLPC','RLAY','VINP','BVS','GSM','OSPN','ABUS','LENZ','BYRN','UFCS','MOFG','HTBK','GLAD','KURA','ORRF','SLP','SFIX','NCMI','HTBI','VALN','MVST','CLNE','LEGH','TRST','BITF','RCAT','ESQ','KRT','PHAR','LPRO','MGIC','VERV','AMRK','CRMD','WASH','CNDT','LIND','ACIC','RZLV','CCBG','MNTK','ANNA','DHC','SCVL','WEST','THFF','CCRN','FLX','NVTS','SHEN','DMRC','CASS','FRPH','NYMT','PPTA','TMCI','SCHL','HNST','FISI','KIDS','LVRO','RMR','MGTX','PGC','NEXN','ABSI','HIFS','OB','API','GLDD','ANAB','EVLV','ACCD','SVRA','DJCO','MNRO','MNMD','IMXI','PGEN','XERS','GRRR','CLMB','LYTS','SPFI','SENEB','SENEA','TREE','MPB','IMTX','ORIC','ODP','CRNC','CMCO','FMNB','RDVT','OMER','CAN','MAZE','SERV','DGICA','JFIN','CGEM','CCNE','ALRS','VREX','EBTC','DGICB','AUTL','CBNK','METC','MERC','GAMB','BCAL','SHBI','TALK','METCB','LINC','ABVX','OPAL','FARO','FLGT','RSVR','GAIN','CCSI','DADA','SRDX','KALV','RNAC','CADL','DDI','NNDM','GRPN','PFIS','BIGC','TCBX','CELC','NFBK','EU','FLWS','DCGO','LASR','LAB','KELYA','KELYB','SWBI','GCL','ARCT','ZJK','SDA','ZSPC','FFIC','MLYS','YORW','DCTH','LGTY','NWPX','HONE','UNTY','SNDL','VLGEA','SBC','HNRG','BAND','SOHU','RAPP','CMRX','GCBC','RICK','TRDA','SKYT','CLPT','GLRE','ALTI','TBPH','ALT','SMLR','HUMA','ALLO','ANSC','WLDN','INV','GMGI','PACB','CLFD','SAGE','INNV','SVC','NRIM','KE','ALNT','KROS','AIRJ','APPS','KMDA','OABI','OUST','MREO','STOK','AROW','PRCH','PROK','CWCO','TECX','RWAY','FBIZ','EYPT','GILT','CVGW','PRTC','UPB','MXCT','KRNY','URGN','ITIC','ZEUS','SIGA','BLDP','TWNP','MITK','GRVY','MFH','BSRR','ANGO','NETD','PERI','BZAI','CDXC','KBSX','VBNK','TITN','FFWM','MSBI','TRVI','NPCE','GOCO','LMNR','ZVRA','ACTG','NATH','TBRG','ORGO','LAND','CMPX','BTBT','GPRE','ERAS','ALDX','DHIL','TKNO','CARE','BMRC','AKBA','HRZN','SHYF','CMPS','HIVE','WALD','NYXH','BWB','PHAT','CCIX','HBCP','ORKA','JYD','ATXS','GLUE','KFII','WTBA','AIP','GDEV','PRME','USCB','HAIN','BSVN','APEI','AURA','ETON','RRBI','KLTR','EWCZ','AHG','FRBA','CDRO','VALU','RDUS','OLMA','GPAT','LSAK','NNOX','ZIMV','ASLE','OFLX','ALF','CRMT','BLZE','MBAV','BWMN','MBX','TBCH','ATYR','PBPB','MTRX','CVRX','NRC','RGNX','ESPR','HIT','NVEC','TCMD','CWBC','DOGZ','LOCO','RIGL','LUNG','LAES','BBCP','EGHT','GEVO','CCIR','LE','MTLS','ACNB','AUDC','POET','CDZI','FMAO','NXXT','ASPI','AMCX','TERN','CDXS','WEYS','AEHR','PMTS','IBEX','SATL','PANL','TTSH','GIG','PSNL','LOVE','ATAI','SPOK','ARTNA','PNRG','NEWT','TSHA','JAKK','LWAY','BRY','MVIS','ZBIO'
                ]

    ds = '{{ ds }}'
    today = date.today()
    current_date_str = today.strftime('%Y%m%d') 
    production_table = f'{schema}.stock_prices_version5'
    staging_table = production_table + '_stg_' + current_date_str + '_v5'
    cumulative_table = f'{schema}.your_table_name'
    yesterday_ds = '{{ yesterday_ds }}'

    # Example date range
    START_DATE_STR = '25-02-2025' # "DD-MM-YYYY" format
    END_DATE_STR   = '26-02-2025'

    def create_iceberg_table_if_not_exists():
        """
        Check if the staging Iceberg table exists; if not, create it.
        """
        try:
            catalog.load_table(staging_table)
            print(f"Table '{staging_table}' already exists. Skipping creation.")
        except NoSuchTableError:
            print(f"Table '{staging_table}' does not exist. Creating now...")
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=2,  # field_id=2 -> 'date' in the schema
                    field_id=1000,
                    transform=DayTransform(),
                    name="datetime_day"
                )
            )
            catalog.create_table(
                identifier=staging_table,
                schema=Schema(
                    NestedField(field_id=1, name="ticker", field_type=StringType(), required=True),
                    NestedField(field_id=2, name="date",   field_type=DateType(),   required=True),
                    NestedField(field_id=3, name="open",   field_type=DoubleType(), required=False),
                    NestedField(field_id=4, name="close",  field_type=DoubleType(), required=False),
                    NestedField(field_id=5, name="volume", field_type=DoubleType(), required=False),
                    NestedField(field_id=6, name="high", field_type=DoubleType(), required=False),
                    NestedField(field_id=7, name="low", field_type=DoubleType(), required=False),
                ),
                partition_spec=partition_spec
            )
            print(f"Table '{staging_table}' created successfully.")
        except Exception as e:
            print(f"Error checking/creating the table '{staging_table}': {e}")

    def fetch_all_stock_data_for_range(
        start_date_str: str, 
        end_date_str: str,
        tickers=None
    ) -> pa.Table:
        """
        Fetch daily data for *all tickers* in the provided date range (inclusive)
        in one pass per ticker (instead of day-by-day). Returns a single
        PyArrow Table containing the entire batch of data.
        
        :param start_date_str: "DD-MM-YYYY"
        :param end_date_str:   "DD-MM-YYYY"
        :param tickers:        List of ticker symbols
        :return:               PyArrow table with (ticker, date, open, close, volume)
        """
        if tickers is None:
            tickers = []

        # Parse the date range
        try:
            start_date = datetime.strptime(start_date_str, "%d-%m-%Y").date()
            end_date   = datetime.strptime(end_date_str, "%d-%m-%Y").date()
        except ValueError as ve:
            print(f"Invalid date format. Please use DD-MM-YYYY. Error: {ve}")
            return pa.Table.from_batches([])  # Return an empty table

        # Ensure start_date <= end_date
        if start_date > end_date:
            print("Start date is after end date. Aborting data fetch.")
            return pa.Table.from_batches([])  # empty

        all_rows = []
        for ticker in tickers:
            # Single API call for the entire date range
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
                f"{start_date.isoformat()}/{end_date.isoformat()}?adjusted=true&sort=asc&apiKey={POLYGON_API_KEY}"
            )
            print(f"[Fetching range data] {ticker}: {url}")

            try:
                resp = requests.get(url)
                resp.raise_for_status()
                raw_json = resp.json()
                results = raw_json.get("results", [])

                for bar in results:
                    ts_millis = bar.get("t")
                    if not ts_millis:
                        continue
                    bar_date = datetime.utcfromtimestamp(ts_millis / 1000).date()
                    
                    row = {
                        "ticker": ticker,
                        "date":   bar_date,
                        "open":   bar.get("o", 0.0),
                        "close":  bar.get("c", 0.0),
                        "volume": bar.get("v", 0.0),
                        'high': bar.get('h', 0.0),
                        'low': bar.get('l', 0.0),
                    }
                    all_rows.append(row)
                    
            except requests.exceptions.RequestException as e:
                # On failed request, push a zero row for each day in the range 
                # or skip entirely. For demonstration, we'll skip:
                print(f"Request failed for ticker {ticker}: {e}")
            except Exception as e:
                print(f"Unexpected error for ticker {ticker}: {e}")

        if not all_rows:
            # If we got nothing at all for all tickers, return an empty table.
            print(f"No data found for any tickers in range {start_date_str} - {end_date_str}")
            return pa.Table.from_batches([])

        arrow_schema = pa.schema([
            pa.field("ticker", pa.string(),  nullable=False),
            pa.field("date",   pa.date32(),  nullable=False),
            pa.field("open",   pa.float64(), nullable=True),
            pa.field("close",  pa.float64(), nullable=True),
            pa.field("volume", pa.float64(), nullable=True),
            pa.field("high", pa.float64(), nullable=True),
            pa.field("low", pa.float64(), nullable=True),
        ])

        return pa.Table.from_pylist(all_rows, schema=arrow_schema)

    def append_data_to_iceberg(arrow_table: pa.Table) -> None:
        """
        Appends a given PyArrow table to the staging Iceberg table.
        """
        if arrow_table is None or arrow_table.num_rows == 0:
            print("[append_data_to_iceberg] No data to append. Skipping.")
            return

        try:
            table = catalog.load_table(staging_table)
            table.append(arrow_table)
            print(f"[append_data_to_iceberg] Appended {arrow_table.num_rows} rows to {staging_table}")
        except Exception as e:
            print(f"[append_data_to_iceberg] Error appending to Iceberg: {e}")

    def main():
        """
        Main function orchestrating the data fetch + append:
        1. Create the Iceberg table if it doesn't exist
        2. Fetch entire range for each ticker in a single pass
        3. Append the entire dataset to the staging table
        """
        create_iceberg_table_if_not_exists()
        big_arrow_table = fetch_all_stock_data_for_range(
            start_date_str=START_DATE_STR,
            end_date_str=END_DATE_STR,
            tickers=MAANG_STOCKS
        )
        append_data_to_iceberg(big_arrow_table)

    #
    # Airflow tasks below
    #

    # 1. Create production table
    create_daily_step = PythonOperator(
        task_id="create_daily_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {production_table} (
                    ticker VARCHAR,
                    date DATE,
                    open DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    rn BIGINT,
                    upload_d DATE
                )
                WITH (
                   format = 'PARQUET'   
                )   
            """.format(production_table=production_table) 
        }
    )

    # 2. Create staging table (Trino version; note we also create an Iceberg table above)
    create_staging_step = PythonOperator(
        task_id="create_staging_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {staging_table} (
                    ticker VARCHAR,
                    date DATE,
                    open DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    high DOUBLE,
                    low DOUBLE
                )
                WITH (
                   format = 'PARQUET'
                )
            """.format(staging_table=staging_table)
        }
    )

    # 3. Load data to staging (Iceberg) in one shot
    load_to_staging_step = PythonOperator(
        task_id="load_polygon_data",
        python_callable=main,
    )

    # 4. Insert into production table
    load_to_production_step = PythonOperator(
        task_id="load_to_production_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {production_table}
                SELECT 
                    ticker,
                    date,
                    open,
                    close,
                    volume,
                    high,
                    low,
                    row_number() OVER (PARTITION BY ticker, date ORDER BY date DESC) AS rn,
                    current_date AS upload_d
                FROM {staging_table}
            """.format(production_table=production_table,staging_table=staging_table) 
        }
    )

    # 5. DQ check
    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_dq,
        op_kwargs={
            'query': f"""
                SELECT * 
                FROM {production_table} 
                WHERE rn > 1
            """.format(production_table=production_table)
        }
    )

    # 6. Clean duplicates
    cleaning_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                DELETE FROM {production_table} 
                WHERE rn > 1
            """.format(production_table=production_table)
        }
    )

    # 7. Drop staging table
    drop_staging_table = PythonOperator(
        task_id="drop_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"DROP TABLE {staging_table}".format(staging_table=staging_table)
        }
    )

    # # 8. Create cumulative table
    # load_to_cumulative_table_from_production = PythonOperator(
    #     task_id="create_cumulative_step",
    #     python_callable=execute_trino_query,
    #     op_kwargs={
    #         'query': """
    #             CREATE TABLE IF NOT EXISTS academy.bigchungus0148.stocks_cumulative_table_v2 (
    #                 ticker VARCHAR,
    #                 c_date date,
    #                 start_date DATE,
    #                 price array(
    #                    row(
    #                       d date,
    #                       open double,
    #                       close double,
    #                       volume bigint
    #                    )
    #                 )
    #             )
    #             WITH (
    #                 format = 'PARQUET',
    #                 partitioning = ARRAY['start_date']
    #             )
    #         """
    #     }
    # )

    # # 9. Cumulative step
    # cumulate_step = PythonOperator(
    #     task_id="cumulate_step",
    #     depends_on_past=True,
    #     python_callable=execute_trino_query,
    #     op_kwargs={
    #         'query': f"""
    #             INSERT INTO academy.bigchungus0148.stocks_cumulative_table_v2 
    #             WITH yesterday AS (
    #                 SELECT * 
    #                 FROM academy.bigchungus0148.stocks_cumulative_table_v2
    #                 WHERE c_date = DATE('2025-01-17')
    #             ),
    #             today AS (
    #                 SELECT *
    #                 FROM {production_table}
    #                 WHERE date = DATE('2025-01-18')
    #                     AND upload_d = DATE('2025-01-28')
    #             )
    #             SELECT
    #                 COALESCE(ls.ticker, ts.ticker) AS ticker,
    #                 COALESCE(ts.date, DATE_ADD('day', 1, ls.c_date)) AS cd,
    #                 COALESCE(ls.start_date, ts.date) AS month_start,
    #                 CASE
    #                     WHEN ts.date IS NULL THEN ls.price
    #                     WHEN ts.date IS NOT NULL AND ls.price IS NULL THEN 
    #                         ARRAY[
    #                             ROW(ts.date, ts.open, ts.close, ts.volume)
    #                         ]
    #                     WHEN ts.date IS NOT NULL AND ls.price IS NOT NULL THEN 
    #                         SLICE(
    #                             CONCAT(
    #                                 ARRAY[ROW(ts.date, ts.open, ts.close, ts.volume)],
    #                                 ls.price
    #                             ), 
    #                             1, 
    #                             7
    #                         )
    #                 END AS prices
    #             FROM yesterday ls
    #             FULL OUTER JOIN today ts 
    #                 ON ls.ticker = ts.ticker
    #         """.format(production_table=production_table)
    #     }
    # )

    create_daily_step >> create_staging_step >> load_to_staging_step >> load_to_production_step >> drop_staging_table >> run_dq_check >> cleaning_step #>> load_to_cumulative_table_from_production >> cumulate_step

daily_raw_data_load_dag_v1()
