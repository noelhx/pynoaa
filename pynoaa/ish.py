__author__ = 'jabaldonedo'

cds_format = [('fill1', [0, 4]), ('id', [4, 10]), ('wban', [10, 15]), ('year', [15, 19]), ('month', [19, 21]),
              ('day', [21, 23]), ('hour', [23, 25]), ('minute', [25, 27]), ('fill2', [27, 60])]

mds_format = [('p_srecd', [60, 63]), ('dir', [60, 63]), ('dirq', [63, 64]), ('dir_type', [64, 65]), ('spd', [65, 69]),
              ('fill2', [69, 70]), ('clg', [70, 75]), ('fill3', [75, 78]), ('vsb', [78, 84]), ('fill4', [84, 87]),
              ('temp_sign', [87, 88]), ('temp', [88, 92]), ('fill5', [92, 93]), ('dewp_sign', [93, 94]),
              ('dewp', [94, 98]), ('fill6', [98, 99]), ('slp', [99, 104]), ('fill7', [104, 105])]

oc1_format = [('gus', [3, 7])]
OC1_LENGTH = 8

gf1_format = [('fill1', [1, 3]), ('skc', [3, 5]), ('fill2', [5, 11]), ('low', [11, 13]), ('fill3', [13, 29]),
              ('med', [20, 22]), ('fill4', [22, 23]), ('hi', [23, 25]), ('fill5', [25, 26])]
GF1_LENGTH = 26

mw_format = [('ww', [3, 5])]
aw_format = [('zz', [3, 5])]
XW_LENGTH = 6

ay1_format = [('pw', [3, 4])]
AY_LENGTH = 8

ma1_format = [('alt', [3, 8]), ('stp', [9, 14])]
MA1_LENGTH = 15

ka1_format = [('code', [6, 7]), ('temp', [7, 12])]
KA1_LENGTH = 13

aax_format = [('hours', [3, 5]), ('pcp', [5, 9]), ('trace', [9, 10])]
AAX_LENGTH = 11

aj1_format = [('sd', [3, 7])]
AJ1_LENGTH = 17

header = "  USAF  WBAN YR--MODAHRMN DIR SPD GUS CLG SKC L M H  VSB MW MW MW MW AW AW AW AW W TEMP DEWP    SLP   ALT " \
    "   STP MAX MIN PCP01 PCP06 PCP24 PCPXX SD\n"
rem_idx = None


class Cds(object):
    prefix = "cds"


class Mds(object):
    prefix = "mds"


class Oc1(object):
    prefix = "oc1"


class Gf1(object):
    prefix = "gf1"


class Ay1(object):
    prefix = "ay1"


class Ma1(object):
    prefix = "ma1"


class Ka1(object):
    prefix = "ka1"


class Pcp(object):
    pcp01 = pcp06 = pcp24 = pcp12 = "*****"
    pcp01t = pcp06t = pcp24t = pcp12t = " "


class Aj1(object):
    prefix = "aj1"


pcp = None


def main():
    convert("./data/data", "./data/data_out")


def convert(input_filename, output_filename):
    global rem_idx, pcp

    with open(input_filename) as fin, open(output_filename, "w") as fout:
        fout.write(header)
        for line in fin:
            rem_idx = line.find("REM")
            if rem_idx == -1:
                rem_idx = 9999

            cds = get_control_data_section(line)
            mds = get_mandatory_data_section(line)
            oc1 = get_oc1(line)
            gf1 = get_gf1(line)
            ay1 = get_ay1(line)
            ma1 = get_ma1(line)
            ka1 = get_ka1(line)

            mw = sorted(
                [getattr(get_xw(line, "MW" + str(i), mw_format, "ww"), "mw" + str(i) + "_ww") for i in range(1, 5)],
                reverse=True)

            aw = sorted(
                [getattr(get_xw(line, "AW" + str(i), mw_format, "zz"), "aw" + str(i) + "_zz") for i in range(1, 5)],
                reverse=True)

            pcp = Pcp()
            [get_aax(line, "AA" + str(i), aax_format) for i in range(1, 5)]

            aj1 = get_aj1(line)

            control_data = "{cds_id} {wban} {year}{month}{day}{hour}{minute} ".format(cds_id=cds.cds_id,
                                                                                      wban=cds.cds_wban,
                                                                                      year=cds.cds_year,
                                                                                      month=cds.cds_month,
                                                                                      day=cds.cds_day,
                                                                                      hour=cds.cds_hour,
                                                                                      minute=cds.cds_minute)

            mandatory_data = " ".join(
                [mds.mds_dir, mds.mds_spd, oc1.oc1_gus, mds.mds_clg, gf1.gf1_skc, gf1.gf1_low, gf1.gf1_med, gf1.gf1_hi,
                 mds.mds_vsb] + mw + aw + [ay1.ay1_pw, mds.mds_temp, mds.mds_dewp, mds.mds_slp, ma1.ma1_alt,
                                           ma1.ma1_stp, ka1.ka1_max_temp, ka1.ka1_min_temp]) + " " + "".join(
                [pcp.pcp01, pcp.pcp01t, pcp.pcp06, pcp.pcp06t, pcp.pcp24, pcp.pcp24t, pcp.pcp12,
                 pcp.pcp12t]) + str(aj1.aj1_sd)



            out_line = control_data + mandatory_data + "\n"
            fout.write(out_line)


def get_control_data_section(line):
    cds = get_data(line, cds_format, Cds())

    if cds.cds_wban == "99999":
        cds.cds_wban = "*****"

    return cds


def get_mandatory_data_section(line):
    line_data = get_data(line, mds_format, Mds())

    # apply rules to certain fields

    if line_data.mds_dir == "999":
        line_data.mds_dir = "***"

    if line_data.mds_dir_type == "V":
        line_data.mds_dir_type = "990"

    if line_data.mds_spd == "9999":
        line_data.mds_spd = "***"
    else:
        line_data.mds_spd = format_blank(int((float(line_data.mds_spd) / 10.) * 2.237 + 0.5), 3)

    if line_data.mds_clg == "99999":
        line_data.mds_clg = "***"
    else:
        if line_data.mds_clg.isdecimal():
            line_data.mds_clg = str(line_data.mds_clg)
        else:
            line_data.mds_clg = format_blank(int((float(line_data.mds_clg) * 3.281) / 100.0 + 0.5), 3)

    if line_data.mds_vsb == "999999":
        line_data.mds_vsb = "****"
    else:
        line_data.mds_vsb = float(line_data.mds_vsb) * 0.000625
        if line_data.mds_vsb > 99.9:
            line_data.mds_vsb = 99.9

        if line_data.mds_vsb > 10.058125:
            line_data.mds_vsb = 10.0

        line_data.mds_vsb = "{:>4}".format(round(line_data.mds_vsb, 1))

    if line_data.mds_temp == "9999":
        line_data.mds_temp = "****"
    else:
        mds = int(line_data.mds_temp)
        if line_data.mds_temp_sign == "-":
            mds = -int(mds)
        if mds < -178:
            mds = int((float(mds) / 10.) * 1.8 + 32. - 0.5)
        else:
            mds = int((float(mds) / 10.) * 1.8 + 32. + 0.5)
        line_data.mds_temp = format_blank(mds, 4)

    if line_data.mds_dewp == "9999":
        line_data.mds_dewp = "****"
    else:
        dewp = int(line_data.mds_dewp)
        if line_data.mds_dewp_sign == "-":
            dewp = -dewp
        if dewp < -178:
            dewp = int((float(dewp) / 10.) * 1.8 + 32. - 0.5)
        else:
            dewp = int((float(dewp) / 10.) * 1.8 + 32 + 0.5)
        line_data.mds_dewp = format_blank(dewp, 4)

    if line_data.mds_slp == "99999":
        line_data.mds_slp = "******"
    else:
        slp = float(line_data.mds_slp) / 10.0
        line_data.mds_slp = "{:>6}".format(round(slp, 1))

    return line_data


def get_oc1(line):
    oc1_idx = line.find("OC1")

    if 0 <= oc1_idx < rem_idx:
        line_data = get_data(line[oc1_idx:oc1_idx + OC1_LENGTH], oc1_format, Oc1())

        if line_data.oc1_gus == "9999":
            line_data.oc1_gus = "***"
        else:
            if line_data.oc1_gus.isdecimal():
                line_data.oc1_gus = format_blank(int((float(line_data.oc1_gus) / 10.) * 2.237 + .5), 3)
            else:
                line_data.oc1_gus = "***"

        return line_data
    else:
        ret = Oc1()
        ret.__setattr__('oc1_gus', '***')
        return ret


def get_gf1(line):
    gf1_idx = line.find("GF1")

    if 0 <= gf1_idx < rem_idx:
        gf1 = line[gf1_idx:gf1_idx + GF1_LENGTH]
        line_data = get_data(gf1, gf1_format, Gf1())

        if line_data.gf1_skc == "99":
            line_data.gf1_skc = "**"
        else:
            if line_data.gf1_skc.isdecimal():
                x = int(line_data.gf1_skc)
                if x == 0:
                    line_data.gf1_skc = "CLR"
                elif 1 <= x <= 4:
                    line_data.gf1_skc = "SCT"
                elif 5 <= x <= 7:
                    line_data.gf1_skc = "BKN"
                elif x == 8:
                    line_data.gf1_skc = "OVC"
                elif x == 9:
                    line_data.gf1_skc = "OBS"
                elif x == 10:
                    line_data.gf1_skc = "POB"
            else:
                line_data.gf1_skc = "**"

        if line_data.gf1_low == "99":
            line_data.gf1_low = "*"
        else:
            line_data.gf1_low = line_data.gf1_low[1:2]

        if line_data.gf1_med == "99":
            line_data.gf1_med = "*"
        else:
            line_data.gf1_med = line_data.gf1_med[1:2]

        if line_data.gf1_hi == "99":
            line_data.gf1_hi = "*"
        else:
            line_data.gf1_hi = line_data.gf1_hi[1:2]

        return line_data
    else:
        ret = Gf1()
        ret.__setattr__("gf1_skc", "***")
        ret.__setattr__("gf1_low", "*")
        ret.__setattr__("gf1_med", "*")
        ret.__setattr__("gf1_hi", "*")
        return ret


def get_xw(line, xw_name, xw_format, prefix):
    mw_idx = line.find(xw_name)

    cls = type(xw_name.title(), (object,), dict(prefix=str.lower(xw_name)))

    if 0 <= mw_idx < rem_idx:
        mw = line[mw_idx:mw_idx + XW_LENGTH]
        line_data = get_data(mw, xw_format, cls)
        return line_data
    else:
        setattr(cls, cls.prefix + "_" + prefix, "**")
        return cls


def get_ay1(line):
    ay_idx = line.find("AY1")

    if 0 <= ay_idx < rem_idx:
        ay = line[ay_idx:ay_idx + AY_LENGTH]
        line_data = get_data(ay, ay1_format, Ay1())
        return line_data
    else:
        ret = Ay1()
        ret.__setattr__("ay1_pw", "*")
        return ret


def get_ma1(line):
    ma1_idx = line.find("MA1")

    if 0 <= ma1_idx < rem_idx:
        ma1 = line[ma1_idx:ma1_idx + MA1_LENGTH]
        line_data = get_data(ma1, ma1_format, Ma1())

        if line_data.ma1_alt == "99999":
            line_data.ma1_alt = "*****"
        else:
            if line_data.ma1_alt.isdecimal():
                line_data.ma1_alt = ((float(line_data.ma1_alt) / 10.0) * 100.0) / 3386.39
                line_data.ma1_alt = "{:>5}".format(round(line_data.ma1_alt, 2))
            else:
                line_data.ma1_alt = "*****"

        if line_data.ma1_stp == "99999":
            line_data.ma1_stp = "******"
        else:
            if line_data.ma1_stp.isdecimal():
                line_data.ma1_stp = float(line_data.ma1_stp) / 10.0
                line_data.ma1_stp = "{:>6}".format(round(line_data.ma1_stp, 1))
            else:
                line_data.ma1_alt = "******"

        return line_data
    else:
        ret = Ma1()
        ret.__setattr__('ma1_alt', "*****")
        ret.__setattr__('ma1_stp', "******")
        return ret


def get_ka1(line):
    ka1_idx = line.find("KA1")

    if 0 <= ka1_idx < rem_idx:
        ka1 = line[ka1_idx:ka1_idx + KA1_LENGTH]
        line_data = get_data(ka1, ka1_format, Ka1())

        if line_data.ka1_temp == "+9999":
            line_data.ka1_temp = "***"
            setattr(line_data, "ka1_max_temp", "***")
            setattr(line_data, "ka1_min_temp", "***")
        else:
            setattr(line_data, "ka1_max_temp", "***")
            setattr(line_data, "ka1_min_temp", "***")
            if line_data.ka1_temp.isdecimal():
                temp = float(line_data.ka1_temp)
                if temp < -178:
                    temp = int((temp / 10.) * 1.8 + 32. - 0.5)
                else:
                    temp = int((temp / 10.) * 1.8 + 32. + 0.5)

                if line_data.ka1_code == "N":
                    setattr(line_data, "ka1_min_temp", "{:>3}".format(temp))

                elif line_data.ka1_code == "M":
                    setattr(line_data, "ka1_max_temp", "{:>3}".format(temp))
            else:
                line_data.ka1_temp = "***"
        return line_data
    else:
        ret = Ka1()
        ret.__setattr__("ka1_max_temp", "***")
        ret.__setattr__("ka1_min_temp", "***")
        return ret


def get_aax(line, aax_name, ax_format):
    idx_aax = line.find(aax_name)

    pcp_id = str.lower(aax_name) + '_pcp'
    hours_id = str.lower(aax_name) + '_hours'
    trace_id = str.lower(aax_name) + '_trace'

    cls = type(aax_name.title(), (object,), dict(prefix=str.lower(aax_name)))

    if 0 <= idx_aax < rem_idx:
        aax = line[idx_aax:idx_aax + AAX_LENGTH]
        line_data = get_data(aax, ax_format, cls)

        pcp_val = str(getattr(line_data, pcp_id))
        hours = str(getattr(line_data, hours_id))
        trace = str(getattr(line_data, trace_id))

        if pcp_val == "9999":
            setattr(line_data, pcp_id, '*****')
        elif pcp.isdecimal():
            set_pcp(float(pcp_val), hours, trace)
        else:
            setattr(line_data, pcp_id, '*****')
        return line_data
    else:
        setattr(cls, pcp_id, '*****')
        return cls


def get_aj1(line):
    aj1_idx = line.find("AJ1")

    if 0 <= aj1_idx < rem_idx:
        aj1 = line[aj1_idx:aj1_idx + AJ1_LENGTH]
        line_data = get_data(aj1, aj1_format, Aj1())

        if line_data.aj1_sd == "9999":
            line_data.aj1_sd = "**"
        else:
            if line_data.aj1_sd.isdecimal():
                line_data.aj1_sd = "{:<2}".format(int(float(line_data.aj1_sd) * 0.3937008 + 0.5))
            else:
                line_data.aj1_sd = "**"
            return line_data
    else:
        ret = Aj1()
        ret.__setattr__('aj1_sd', '**')
        return ret


def get_data(line, format_data, ret):
    for data in format_data:
        setattr(ret, ret.prefix + "_" + data[0], line[data[1][0]:data[1][1]])
    return ret


def set_pcp(value, hours, trace):
    global pcp

    value = (value / 10.) * 0.03937008

    if hours == "01":
        pcp.pcp01 = "{:<6}".format(round(value, 2))
        if trace == "2":
            pcp.pcp01t = "T"
    elif hours == "06":
        pcp.pcp06 = "{:<6}".format(round(value, 2))
        if trace == "2":
            pcp.pcp06t = "T"
    elif hours == "24":
        pcp.pcp24 = "{:<6}".format(round(value, 2))
        pcp.pcp24t = "T"
    else:
        pcp.pcp12 = "{:<6}".format(round(value, 2))
        pcp.pcp12t = "T"


def format_blank(i, length):
    blanks = "                 "
    s = str(i)
    if len(s) < length:
        s = blanks[0:length - len(s)] + s

    return s


if __name__ == "__main__":
    main()