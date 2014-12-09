import scraperwiki
import lxml.html
import re, datetime

def Parse(odata):
    root = lxml.html.fromstring(odata["rawhtml"])

    data = { "NOTAM":odata["NOTAM"], "lat":odata["lat"], "lng":odata["lng"], "Type":odata["Type"] }
    data["rawhtml"] = odata["rawhtml"]
    missedsrows = [ ]
    arealetter = "_"
    rdata = { "_":{ "arealetter":"_", "NOTAM":odata["NOTAM"] } }
    for row in root.cssselect("tr"):
        if len(row.cssselect("tr tr")):
            continue
        td0 = len(row) and (row[0].text_content().strip() or ' ')
        srow = "///".join(td.text_content().strip()  for td in row  if td.text_content().strip())
        if not srow:
            continue

        mjumpto = re.match("Jump To:///Affected Areas\s*Operating Restrictions and Requirements\s*Other Information", srow)
        mdate = re.match("(Issue Date|Beginning Date and Time|Beginning Date and Time|Ending Date and Time)\s*:///(.*)", srow)
        mnotam = re.match("NOTAM Number\s*:///FDC\s*([\d/]+)\s*Download shapefiles$", srow)
        mlocation = re.match("Location\s*:///(.*)$", srow)
        mtype = re.match("Type\s*:///(.*)$", srow)
        mreplaced = re.match("Replaced NOTAM\(s\)\s*:///(.*)$", srow)
        mother = re.match("(Pilots May Contact|Point of Contact|ARTCC|Authority|Reason for NOTAM|Altitude)\s*:///(.*)$", srow)
        mothertop = re.match("(Other Information|Affected Area\(s\)|Operating Restrictions and Requirements):?///Top", srow)
        mcenter = re.match("""Center\s*:///.*\(Latitude: (\d+)\xba(\d+)'(\d+)"N, Longitude: (\d+)\xba(\d+)'(\d+)"W\)""", srow)
        mradius = re.match("Radius\s*:///(\d+) nautical miles", srow)
        marealetter = re.match("Area ([A-Z])$", srow)
        mfromto = re.match('(From|To)\s*(\w* \d+, \d\d\d\d at \d\d\d\d UTC)', srow)

        if mjumpto or mothertop:
            pass
        elif mnotam:
            data["NOTAM_Number"] = mnotam.group(1)
        elif mlocation:
            data["Location"] = mlocation.group(1).strip()
            loc = data["Location"]
            loc = re.sub("\s+near\s+[A-Z\s()/']*$", "", loc)
            loc = re.sub(".*,\s*", "", loc)
            data["state"] = loc
        elif mtype:
            data["DType"] = mtype.group(1)
        elif mreplaced:
            if mreplaced.group(1) != 'N/A':
                data["Replaced"] = mreplaced.group(1)
        elif mother:
            data[mother.group(1).replace(" ", "_")] = mother.group(2)
            if data["Type"] == "HAZARDS" and re.search("fire fighting(?i)", mother.group(2)):
                data["Type"] = "FIRE"

        elif marealetter:
            arealetter = marealetter.group(1) 
            rdata[arealetter] = { "arealetter":arealetter, "NOTAM":odata["NOTAM"] }
        elif mcenter:
            rdata[arealetter]["center_lat"] = int(mcenter.group(1)) + int(mcenter.group(2))/60.0 + int(mcenter.group(3))/3600.0
            rdata[arealetter]["center_lng"] = -(int(mcenter.group(4)) + int(mcenter.group(5))/60.0 + int(mcenter.group(6))/3600.0)
        elif mradius:
            rdata[arealetter]["Radius_NM"] = int(mradius.group(1))
        elif mfromto:
            data[mfromto.group(1)+arealetter] = datetime.datetime.strptime(mfromto.group(2), "%B %d, %Y at %H%M UTC")

        elif mdate:
            key = mdate.group(1).replace(" ", "_")
            dval, val = None, mdate.group(2)
            if re.match("Effective Immediately", val):
                dval = datetime.datetime.strptime(odata["Date"], '%Y-%m-%d').date()
            elif re.match("Until further notice", val):
                pass
            elif re.match("\w+ \d\d, \d\d\d\d UTC", val):
                dval = datetime.datetime.strptime(val.split("UTC", 1)[0], "%B %d, %Y ")
            else:
                try:
                    dval = datetime.datetime.strptime(val, "%B %d, %Y at %H%M UTC")
                    val = None
                except ValueError:
                    print val
            if dval:
                data[key] = dval
            if val:
                data[key+"_desc"] = val
        else:
            if re.search("\(USSS\)", srow):
                if re.search("vice president(?i)", srow):
                    data["Type"] = "VP"
                else:
                    data["Type"] = "USSS"

            #print [srow]
            missedsrows.append(srow)
    #print "***", missedsrows[-1]

    return data, rdata

scraperwiki.sqlite.attach("notam_list")
scraperwiki.sqlite.execute("drop table if exists parsed_notams")
offs, step = 0, 150
sql = "* from notamtable left join notamdetails on notamdetails.NOTAM=notamtable.NOTAM where rawhtml is not null"
states = set()
while True:
    ldata = [ ]
    lrdata = [ ]
    odatas = scraperwiki.sqlite.select("%s limit %d offset %d" % (sql, step, offs))
    for odata in odatas:
        data, rdata = Parse(odata)
        states.add(data.get("state"))
        ldata.append(data)
        lrdata.extend(rdata.values())
    scraperwiki.sqlite.save(["NOTAM"], ldata, "pnotam")
    scraperwiki.sqlite.save(["NOTAM", "arealetter"], lrdata, "pnotamareas")
    if len(odatas) != step:
        break
    offs += step
print sorted(list(states))




