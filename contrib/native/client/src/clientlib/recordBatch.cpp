#include <boost/log/trivial.hpp>

#include "common.hpp"
#include "recordBatch.hpp"

using namespace common;
using namespace exec;
using namespace exec::user;
using namespace Drill;

const uint32_t YEARS_TO_MONTHS=12;
const uint32_t HOURS_TO_MILLIS=60*60*1000;
const uint32_t MINUTES_TO_MILLIS=60*1000;
const uint32_t SECONDS_TO_MILLIS=1000;

static char timezoneMap[][36]={
        "Africa/Abidjan", "Africa/Accra", "Africa/Addis_Ababa", "Africa/Algiers", "Africa/Asmara", "Africa/Asmera",
        "Africa/Bamako", "Africa/Bangui", "Africa/Banjul", "Africa/Bissau", "Africa/Blantyre", "Africa/Brazzaville",
        "Africa/Bujumbura", "Africa/Cairo", "Africa/Casablanca", "Africa/Ceuta", "Africa/Conakry", "Africa/Dakar",
        "Africa/Dar_es_Salaam", "Africa/Djibouti", "Africa/Douala", "Africa/El_Aaiun", 
        "Africa/Freetown", "Africa/Gaborone",
        "Africa/Harare", "Africa/Johannesburg", "Africa/Juba", "Africa/Kampala", "Africa/Khartoum", "Africa/Kigali",
        "Africa/Kinshasa", "Africa/Lagos", "Africa/Libreville", "Africa/Lome", "Africa/Luanda", "Africa/Lubumbashi",
        "Africa/Lusaka", "Africa/Malabo", "Africa/Maputo", "Africa/Maseru", "Africa/Mbabane", "Africa/Mogadishu",
        "Africa/Monrovia", "Africa/Nairobi", "Africa/Ndjamena", "Africa/Niamey", 
        "Africa/Nouakchott", "Africa/Ouagadougou",
        "Africa/Porto-Novo", "Africa/Sao_Tome", "Africa/Timbuktu", "Africa/Tripoli", 
        "Africa/Tunis", "Africa/Windhoek",
        "America/Adak", "America/Anchorage", "America/Anguilla", "America/Antigua", "America/Araguaina",
        "America/Argentina/Buenos_Aires", "America/Argentina/Catamarca", "America/Argentina/ComodRivadavia",
        "America/Argentina/Cordoba", "America/Argentina/Jujuy", "America/Argentina/La_Rioja",
        "America/Argentina/Mendoza", "America/Argentina/Rio_Gallegos", "America/Argentina/Salta",
        "America/Argentina/San_Juan", "America/Argentina/San_Luis", "America/Argentina/Tucuman",
        "America/Argentina/Ushuaia", "America/Aruba", "America/Asuncion", "America/Atikokan", "America/Atka",
        "America/Bahia", "America/Bahia_Banderas", "America/Barbados", "America/Belem", "America/Belize",
        "America/Blanc-Sablon", "America/Boa_Vista", "America/Bogota", "America/Boise", "America/Buenos_Aires",
        "America/Cambridge_Bay", "America/Campo_Grande", "America/Cancun", "America/Caracas", "America/Catamarca",
        "America/Cayenne", "America/Cayman", "America/Chicago", "America/Chihuahua", "America/Coral_Harbour",
        "America/Cordoba", "America/Costa_Rica", "America/Cuiaba", "America/Curacao", "America/Danmarkshavn",
        "America/Dawson", "America/Dawson_Creek", "America/Denver", "America/Detroit", "America/Dominica",
        "America/Edmonton", "America/Eirunepe", "America/El_Salvador", "America/Ensenada", "America/Fort_Wayne",
        "America/Fortaleza", "America/Glace_Bay", "America/Godthab", "America/Goose_Bay", "America/Grand_Turk",
        "America/Grenada", "America/Guadeloupe", "America/Guatemala", "America/Guayaquil", "America/Guyana",
        "America/Halifax", "America/Havana", "America/Hermosillo", 
        "America/Indiana/Indianapolis", "America/Indiana/Knox",
        "America/Indiana/Marengo", "America/Indiana/Petersburg", "America/Indiana/Tell_City",
        "America/Indiana/Vevay", "America/Indiana/Vincennes", "America/Indiana/Winamac", 
        "America/Indianapolis", "America/Inuvik",
        "America/Iqaluit", "America/Jamaica", "America/Jujuy", "America/Juneau", "America/Kentucky/Louisville",
        "America/Kentucky/Monticello", "America/Knox_IN", "America/Kralendijk", "America/La_Paz", "America/Lima",
        "America/Los_Angeles", "America/Louisville", "America/Lower_Princes", "America/Maceio", "America/Managua",
        "America/Manaus", "America/Marigot", "America/Martinique", "America/Matamoros", "America/Mazatlan",
        "America/Mendoza", "America/Menominee", "America/Merida", "America/Metlakatla", "America/Mexico_City",
        "America/Miquelon", "America/Moncton", "America/Monterrey", "America/Montevideo", "America/Montreal",
        "America/Montserrat", "America/Nassau", "America/New_York", "America/Nipigon", 
        "America/Nome", "America/Noronha",
        "America/North_Dakota/Beulah", "America/North_Dakota/Center", "America/North_Dakota/New_Salem",
        "America/Ojinaga", "America/Panama", "America/Pangnirtung",
        "America/Paramaribo", "America/Phoenix", "America/Port-au-Prince",
        "America/Port_of_Spain", "America/Porto_Acre", "America/Porto_Velho",
        "America/Puerto_Rico", "America/Rainy_River", "America/Rankin_Inlet",
        "America/Recife", "America/Regina", "America/Resolute", "America/Rio_Branco", 
        "America/Rosario", "America/Santa_Isabel",
        "America/Santarem", "America/Santiago", "America/Santo_Domingo",
        "America/Sao_Paulo", "America/Scoresbysund", "America/Shiprock", "America/Sitka", 
        "America/St_Barthelemy", "America/St_Johns",
        "America/St_Kitts", "America/St_Lucia", "America/St_Thomas",
        "America/St_Vincent", "America/Swift_Current", "America/Tegucigalpa",
        "America/Thule", "America/Thunder_Bay", "America/Tijuana", "America/Toronto", 
        "America/Tortola", "America/Vancouver",
        "America/Virgin", "America/Whitehorse", "America/Winnipeg", "America/Yakutat", 
        "America/Yellowknife", "Antarctica/Casey",
        "Antarctica/Davis", "Antarctica/DumontDUrville", "Antarctica/Macquarie",
        "Antarctica/Mawson", "Antarctica/McMurdo", "Antarctica/Palmer",
        "Antarctica/Rothera", "Antarctica/South_Pole", "Antarctica/Syowa",
        "Antarctica/Vostok", "Arctic/Longyearbyen", "Asia/Aden", "Asia/Almaty", "Asia/Amman", "Asia/Anadyr",
        "Asia/Aqtau", "Asia/Aqtobe", "Asia/Ashgabat", "Asia/Ashkhabad", "Asia/Baghdad", "Asia/Bahrain",
        "Asia/Baku", "Asia/Bangkok", "Asia/Beirut", "Asia/Bishkek", "Asia/Brunei", "Asia/Calcutta",
        "Asia/Choibalsan", "Asia/Chongqing", "Asia/Chungking", "Asia/Colombo", "Asia/Dacca", "Asia/Damascus",
        "Asia/Dhaka", "Asia/Dili", "Asia/Dubai", "Asia/Dushanbe", "Asia/Gaza", "Asia/Harbin",
        "Asia/Hebron", "Asia/Ho_Chi_Minh", "Asia/Hong_Kong", "Asia/Hovd", "Asia/Irkutsk", "Asia/Istanbul",
        "Asia/Jakarta", "Asia/Jayapura", "Asia/Jerusalem", "Asia/Kabul", "Asia/Kamchatka", "Asia/Karachi",
        "Asia/Kashgar", "Asia/Kathmandu", "Asia/Katmandu", "Asia/Kolkata", "Asia/Krasnoyarsk", "Asia/Kuala_Lumpur",
        "Asia/Kuching", "Asia/Kuwait", "Asia/Macao", "Asia/Macau", "Asia/Magadan", "Asia/Makassar",
        "Asia/Manila", "Asia/Muscat", "Asia/Nicosia", "Asia/Novokuznetsk", "Asia/Novosibirsk", "Asia/Omsk",
        "Asia/Oral", "Asia/Phnom_Penh", "Asia/Pontianak", "Asia/Pyongyang", "Asia/Qatar", "Asia/Qyzylorda",
        "Asia/Rangoon", "Asia/Riyadh", "Asia/Saigon", "Asia/Sakhalin", "Asia/Samarkand", "Asia/Seoul",
        "Asia/Shanghai", "Asia/Singapore", "Asia/Taipei", "Asia/Tashkent", "Asia/Tbilisi", "Asia/Tehran",
        "Asia/Tel_Aviv", "Asia/Thimbu", "Asia/Thimphu", "Asia/Tokyo", "Asia/Ujung_Pandang", "Asia/Ulaanbaatar",
        "Asia/Ulan_Bator", "Asia/Urumqi", "Asia/Vientiane", "Asia/Vladivostok", "Asia/Yakutsk", "Asia/Yekaterinburg",
        "Asia/Yerevan", "Atlantic/Azores", "Atlantic/Bermuda", "Atlantic/Canary", 
        "Atlantic/Cape_Verde", "Atlantic/Faeroe",
        "Atlantic/Faroe", "Atlantic/Jan_Mayen", "Atlantic/Madeira",
        "Atlantic/Reykjavik", "Atlantic/South_Georgia", "Atlantic/St_Helena",
        "Atlantic/Stanley", "Australia/ACT", "Australia/Adelaide", "Australia/Brisbane", 
        "Australia/Broken_Hill", "Australia/Canberra",
        "Australia/Currie", "Australia/Darwin", "Australia/Eucla", "Australia/Hobart", 
        "Australia/LHI", "Australia/Lindeman",
        "Australia/Lord_Howe", "Australia/Melbourne", "Australia/NSW", "Australia/North", 
        "Australia/Perth", "Australia/Queensland",
        "Australia/South", "Australia/Sydney", "Australia/Tasmania", "Australia/Victoria", 
        "Australia/West", "Australia/Yancowinna",
        "Brazil/Acre", "Brazil/DeNoronha", "Brazil/East", "Brazil/West", "CET", "CST6CDT",
        "Canada/Atlantic", "Canada/Central", "Canada/East-Saskatchewan", "Canada/Eastern", 
        "Canada/Mountain", "Canada/Newfoundland",
        "Canada/Pacific", "Canada/Saskatchewan", "Canada/Yukon", "Chile/Continental", "Chile/EasterIsland", "Cuba",
        "EET", "EST", "EST5EDT", "Egypt", "Eire", "Etc/GMT", "Etc/GMT+0", "Etc/GMT+1", "Etc/GMT+10",
        "Etc/GMT+11", "Etc/GMT+12", "Etc/GMT+2", "Etc/GMT+3", "Etc/GMT+4", "Etc/GMT+5", "Etc/GMT+6", 
        "Etc/GMT+7", "Etc/GMT+8",
        "Etc/GMT+9", "Etc/GMT-0", "Etc/GMT-1", "Etc/GMT-10", "Etc/GMT-11", "Etc/GMT-12", 
        "Etc/GMT-13", "Etc/GMT-14", "Etc/GMT-2",
        "Etc/GMT-3", "Etc/GMT-4", "Etc/GMT-5", "Etc/GMT-6", "Etc/GMT-7", "Etc/GMT-8", 
        "Etc/GMT-9", "Etc/GMT0", "Etc/Greenwich",
        "Etc/UCT", "Etc/UTC", "Etc/Universal", "Etc/Zulu", "Europe/Amsterdam", "Europe/Andorra",
        "Europe/Athens", "Europe/Belfast", "Europe/Belgrade", "Europe/Berlin", "Europe/Bratislava", "Europe/Brussels",
        "Europe/Bucharest", "Europe/Budapest", "Europe/Chisinau",
        "Europe/Copenhagen", "Europe/Dublin", "Europe/Gibraltar", "Europe/Guernsey", 
        "Europe/Helsinki", "Europe/Isle_of_Man",
        "Europe/Istanbul", "Europe/Jersey", "Europe/Kaliningrad", "Europe/Kiev", "Europe/Lisbon", "Europe/Ljubljana",
        "Europe/London", "Europe/Luxembourg", "Europe/Madrid", "Europe/Malta", "Europe/Mariehamn", "Europe/Minsk",
        "Europe/Monaco", "Europe/Moscow", "Europe/Nicosia", "Europe/Oslo", "Europe/Paris", "Europe/Podgorica",
        "Europe/Prague", "Europe/Riga", "Europe/Rome", "Europe/Samara", "Europe/San_Marino", "Europe/Sarajevo",
        "Europe/Simferopol", "Europe/Skopje", "Europe/Sofia", "Europe/Stockholm", "Europe/Tallinn", "Europe/Tirane",
        "Europe/Tiraspol", "Europe/Uzhgorod", "Europe/Vaduz", "Europe/Vatican", "Europe/Vienna", "Europe/Vilnius",
        "Europe/Volgograd", "Europe/Warsaw", "Europe/Zagreb", "Europe/Zaporozhye", "Europe/Zurich", "GB",
        "GB-Eire", "GMT", "GMT+0", "GMT-0", "GMT0", "Greenwich", "HST", "Hongkong", "Iceland",
        "Indian/Antananarivo", "Indian/Chagos", "Indian/Christmas",
        "Indian/Cocos", "Indian/Comoro", "Indian/Kerguelen", "Indian/Mahe", "Indian/Maldives", "Indian/Mauritius",
        "Indian/Mayotte", "Indian/Reunion", "Iran", "Israel", "Jamaica", "Japan", "Kwajalein", "Libya", "MET",
        "MST", "MST7MDT", "Mexico/BajaNorte", "Mexico/BajaSur", "Mexico/General", "NZ", "NZ-CHAT", "Navajo", "PRC",
        "PST8PDT", "Pacific/Apia", "Pacific/Auckland", "Pacific/Chatham", "Pacific/Chuuk", "Pacific/Easter",
        "Pacific/Efate", "Pacific/Enderbury", "Pacific/Fakaofo", "Pacific/Fiji", 
        "Pacific/Funafuti", "Pacific/Galapagos",
        "Pacific/Gambier", "Pacific/Guadalcanal", "Pacific/Guam", "Pacific/Honolulu", 
        "Pacific/Johnston", "Pacific/Kiritimati",
        "Pacific/Kosrae", "Pacific/Kwajalein", "Pacific/Majuro", "Pacific/Marquesas", 
        "Pacific/Midway", "Pacific/Nauru",
        "Pacific/Niue", "Pacific/Norfolk", "Pacific/Noumea", "Pacific/Pago_Pago", 
        "Pacific/Palau", "Pacific/Pitcairn",
        "Pacific/Pohnpei", "Pacific/Ponape", "Pacific/Port_Moresby", "Pacific/Rarotonga", 
        "Pacific/Saipan", "Pacific/Samoa",
        "Pacific/Tahiti", "Pacific/Tarawa", "Pacific/Tongatapu", "Pacific/Truk", "Pacific/Wake", "Pacific/Wallis",
        "Pacific/Yap", "Poland", "Portugal", "ROC", "ROK", "Singapore", "Turkey", "UCT", "US/Alaska", "US/Aleutian",
        "US/Arizona", "US/Central", "US/East-Indiana", "US/Eastern", "US/Hawaii", "US/Indiana-Starke",
        "US/Michigan", "US/Mountain", "US/Pacific", "US/Pacific-New", "US/Samoa", 
        "UTC", "Universal", "W-SU", "WET", "Zulu"
};

ValueVectorBase* ValueVectorFactory::allocateValueVector(const FieldMetadata & f, SlicedByteBuf* b){
    const FieldDef& fieldDef = f.def();
    const MajorType& majorType=fieldDef.major_type();
    int type = majorType.minor_type();
    int mode = majorType.mode();

    switch (mode) {

    case DM_REQUIRED:
        switch (type) {
            case TINYINT:
                return new ValueVectorFixed<int8_t>(b,f.value_count());
            case SMALLINT:
                return new ValueVectorFixed<int16_t>(b,f.value_count());
            case INT:
                return new ValueVectorFixed<int32_t>(b,f.value_count());
            case BIGINT:
                return new ValueVectorFixed<int64_t>(b,f.value_count());
            case FLOAT4:
                return new ValueVectorFixed<float>(b,f.value_count());
            case FLOAT8:
                return new ValueVectorFixed<double>(b,f.value_count());
            // Decimal digits, width, max precision is defined in
            // /exec/java-exec/src/main/codegen/data/ValueVectorTypes.tdd
            // Decimal Design Document: http://bit.ly/drilldecimal
            case DECIMAL9:
                return new ValueVectorDecimal9(b,f.value_count(), majorType.scale());
            case DECIMAL18:
                return new ValueVectorDecimal18(b,f.value_count(), majorType.scale());
            case DECIMAL28DENSE:
                return new ValueVectorDecimal28Dense(b,f.value_count(), majorType.scale());
            case DECIMAL38DENSE:
                return new ValueVectorDecimal38Dense(b,f.value_count(), majorType.scale());
            case DECIMAL28SPARSE:
                return new ValueVectorDecimal28Sparse(b,f.value_count(), majorType.scale());
            case DECIMAL38SPARSE:
                return new ValueVectorDecimal38Sparse(b,f.value_count(), majorType.scale());
            case DATE:
                return new ValueVectorTyped<DateHolder, uint64_t>(b,f.value_count());
            case TIMESTAMP:
                return new ValueVectorTyped<DateTimeHolder, uint64_t>(b,f.value_count());
            case TIME:
                return new ValueVectorTyped<TimeHolder, uint32_t>(b,f.value_count());
            case TIMESTAMPTZ:
                return new ValueVectorTypedComposite<DateTimeTZHolder>(b,f.value_count());
            case INTERVAL:
                return new ValueVectorTypedComposite<IntervalHolder>(b,f.value_count());
            case INTERVALDAY:
                return new ValueVectorTypedComposite<IntervalDayHolder>(b,f.value_count());
            case INTERVALYEAR:
                return new ValueVectorTypedComposite<IntervalYearHolder>(b,f.value_count());
            case BIT:
                return new ValueVectorBit(b,f.value_count());
            case VARBINARY:
                return new ValueVectorVarBinary(b, f.value_count()); 
            case VARCHAR:
                return new ValueVectorVarChar(b, f.value_count()); 
            case MONEY:
            default:
                return new ValueVectorUnimplemented(b, f.value_count()); 
        }
    case DM_OPTIONAL:
        switch (type) {
            case TINYINT:
                return new NullableValueVectorFixed<int8_t>(b,f.value_count());
            case SMALLINT:
                return new NullableValueVectorFixed<int16_t>(b,f.value_count());
            case INT:
                return new NullableValueVectorFixed<int32_t>(b,f.value_count());
            case BIGINT:
                return new NullableValueVectorFixed<int64_t>(b,f.value_count());
            case FLOAT4:
                return new NullableValueVectorFixed<float>(b,f.value_count());
            case FLOAT8:
                return new NullableValueVectorFixed<double>(b,f.value_count());
            case DATE:
                return new NullableValueVectorTyped<DateHolder, 
                       ValueVectorTyped<DateHolder, uint64_t> >(b,f.value_count());
            case TIMESTAMP:
                return new NullableValueVectorTyped<DateTimeHolder, 
                       ValueVectorTyped<DateTimeHolder, uint64_t> >(b,f.value_count());
            case TIME:
                return new NullableValueVectorTyped<TimeHolder,
                    ValueVectorTyped<TimeHolder, uint32_t> >(b,f.value_count());
            case TIMESTAMPTZ:
                return new NullableValueVectorTyped<DateTimeTZHolder, 
                       ValueVectorTypedComposite<DateTimeTZHolder> >(b,f.value_count());
            case INTERVAL:
                return new NullableValueVectorTyped<IntervalHolder, 
                       ValueVectorTypedComposite<IntervalHolder> >(b,f.value_count());
            case INTERVALDAY:
                return new NullableValueVectorTyped<IntervalDayHolder, 
                       ValueVectorTypedComposite<IntervalDayHolder> >(b,f.value_count());
            case INTERVALYEAR:
                return new NullableValueVectorTyped<IntervalYearHolder, 
                       ValueVectorTypedComposite<IntervalYearHolder> >(b,f.value_count());
            case BIT:
                return new NullableValueVectorTyped<uint8_t, 
                       ValueVectorBit >(b,f.value_count());
            case VARBINARY:
                //TODO: Varbinary is untested
                return new NullableValueVectorTyped<VarWidthHolder, ValueVectorVarBinary >(b,f.value_count());
            case VARCHAR:
                return new NullableValueVectorTyped<VarWidthHolder, ValueVectorVarChar >(b,f.value_count());
            // not implemented yet
            default:
                return new ValueVectorUnimplemented(b, f.value_count()); 
        }
    case DM_REPEATED:
        switch (type) {
             // not implemented yet
            default:
                return new ValueVectorUnimplemented(b, f.value_count()); 
        }
    }
    return new ValueVectorUnimplemented(b, f.value_count()); 
}


int FieldBatch::load(){
    const FieldMetadata& fmd = this->m_fieldMetadata;
    this->m_pValueVector=ValueVectorFactory::allocateValueVector(fmd, this->m_pFieldData);
    return 0;
}

int RecordBatch::build(){
    // For every Field, get the corresponding SlicedByteBuf.
    // Create a Materialized field. Set the Sliced Byted Buf to the correct slice. 
    // Set the Field Metadata.
    // Load the vector.(Load creates a valuevector object of the correct type:
    //    Use ValueVectorFactory(type) to create the right type. 
    //    Create a Value Vector of the Sliced Byte Buf. 
    // Add the field batch to vector
    size_t startOffset=0;
    //TODO: handle schema changes here. Call a client provided callback?
    for(int i=0; i<this->m_numFields; i++){
        const FieldMetadata& fmd=this->m_pRecordBatchDef->field(i);
        size_t len=fmd.buffer_length();
        FieldBatch* pField = new FieldBatch(fmd, this->m_buffer, startOffset, len) ;
        startOffset+=len;
        pField->load(); // set up the value vectors
        this->m_fields.push_back(pField);
        this->m_fieldDefs.push_back(&fmd);
    }
    return 0;
}

void RecordBatch::print(size_t num){
    std::string nameList;
    for(std::vector<FieldBatch*>::size_type i = 0; i != this->m_fields.size(); i++) {
        FieldMetadata fmd=this->getFieldMetadata(i);
        std::string name= fmd.def().name(0).name();
        nameList+=name;
        nameList+="    ";
    } 
    int numToPrint=this->m_numRecords;
    if(num>0 && num<numToPrint)numToPrint=num;
    BOOST_LOG_TRIVIAL(trace) << nameList;
    std::string values;
    for(size_t n=0; n<numToPrint; n++){
        values="";
        for(std::vector<FieldBatch*>::size_type i = 0; i != this->m_fields.size(); i++) {
            const ValueVectorBase * v = m_fields[i]->getVector();
            char valueBuf[1024+1];
            memset(valueBuf, 0, sizeof(valueBuf)*sizeof(char));
            if(v->isNull(n)){
                strncpy(valueBuf,"null", (sizeof(valueBuf)-1)*sizeof(char));
            } else{
                v->getValueAt(n, valueBuf, (sizeof(valueBuf)-1)*sizeof(char));
            }
            values+=valueBuf;
            values+="    ";
        } 
        BOOST_LOG_TRIVIAL(trace) << values;
    }
}

void DateHolder::load(){
    m_year=1970;
    m_month=1;
    m_day=1;

    time_t  t= m_datetime/1000; // number of seconds since beginning of the Unix Epoch.
    struct tm * tm = gmtime(&t);
    m_year=tm->tm_year+1900;
    m_month=tm->tm_mon+1;
    m_day=tm->tm_mday;
}

std::string DateHolder::toString(){
    std::stringstream sstr;
    sstr << m_year << "-" << m_month << "-" << m_day;
    return sstr.str();
};

void TimeHolder::load(){
    m_hr=0;
    m_min=0;
    m_sec=0;
    m_msec=0;

    time_t  t= m_datetime/1000; // number of seconds since beginning of the Unix Epoch.
    struct tm * tm = gmtime(&t);
    m_hr=tm->tm_hour;
    m_min=tm->tm_min;
    m_sec=tm->tm_sec;
    m_msec=m_datetime%1000;
}

std::string TimeHolder::toString(){
    std::stringstream sstr;
    sstr << m_hr <<":" << m_min<<":"<<m_sec<<"."<<m_msec;
    return sstr.str();
};

void DateTimeHolder::load(){
    m_year=1970;
    m_month=1;
    m_day=1;
    m_hr=0;
    m_min=0;
    m_sec=0;
    m_msec=0;

    time_t  t= m_datetime/1000; // number of seconds since beginning of the Unix Epoch.
    struct tm * tm = gmtime(&t);
    m_year=tm->tm_year+1900;
    m_month=tm->tm_mon+1;
    m_day=tm->tm_mday;
    m_hr=tm->tm_hour;
    m_min=tm->tm_min;
    m_sec=tm->tm_sec;
    m_msec=m_datetime%1000;
}

std::string DateTimeHolder::toString(){
    //TODO: Allow config flag to set delimiter
    std::stringstream sstr;
    sstr << m_year << "-" << m_month << "-" << m_day << " " << m_hr <<":" << m_min<<":"<<m_sec<<"."<<m_msec;
    return sstr.str();
};

void DateTimeTZHolder::load(){
    DateTimeHolder::load();
}

std::string DateTimeTZHolder::toString(){
    std::stringstream sstr;
    sstr << m_year << "-" << m_month << "-" << m_day << " " << m_hr <<":" << m_min<<":"<<m_sec<<"."<<m_msec;
    sstr << "["<<timezoneMap[m_tzIndex]<<"]";
    return sstr.str();
};

std::string IntervalYearHolder::toString(){
    std::stringstream sstr;

    uint32_t years  = (m_month / YEARS_TO_MONTHS);
    uint32_t months = (m_month % YEARS_TO_MONTHS);

    sstr << years << "-" << months;
    return sstr.str();
};

std::string IntervalDayHolder::toString(){
    std::stringstream sstr;

    uint32_t hours  = m_ms / (HOURS_TO_MILLIS);
    uint32_t millis     = m_ms % (HOURS_TO_MILLIS);

    uint32_t minutes = millis / (MINUTES_TO_MILLIS);
    millis      = millis % (MINUTES_TO_MILLIS);

    uint32_t seconds = millis / (SECONDS_TO_MILLIS);
    millis      = millis % (SECONDS_TO_MILLIS);

    sstr << m_day<< " " << hours << ":"<<minutes<<":"<<seconds<<"."<<millis;
    return sstr.str();
};

std::string IntervalHolder::toString(){
    std::stringstream sstr;

    uint32_t years  = (m_month / YEARS_TO_MONTHS);
    uint32_t months = (m_month % YEARS_TO_MONTHS);

    uint32_t hours  = m_ms / (HOURS_TO_MILLIS);
    uint32_t millis     = m_ms % (HOURS_TO_MILLIS);

    uint32_t minutes = millis / (MINUTES_TO_MILLIS);
    millis      = millis % (MINUTES_TO_MILLIS);

    uint32_t seconds = millis / (SECONDS_TO_MILLIS);
    millis      = millis % (SECONDS_TO_MILLIS);

    sstr << years << "-" << months<< "-" << m_day<< " " << hours << ":"<<minutes<<":"<<seconds<<"."<<millis;
    return sstr.str();
};
