const pptxgen = require("pptxgenjs");
const p = new pptxgen();
p.layout = "LAYOUT_WIDE";           // 13.333 x 7.5
const W = 13.333, H = 7.5;

// ---- palette (Ocean) ----
const INK   = "12212E";   // near-black text
const DEEP  = "065A82";   // deep blue (primary)
const TEAL  = "1C7293";   // teal (secondary)
const MID   = "0E1B2E";   // midnight (dark bg)
const AMBER = "E8963A";   // accent
const MUTE  = "5C6E7C";   // muted text
const LIGHT = "FFFFFF";
const TINT  = "EEF4F8";   // card tint
const TINT2 = "E3EEF3";   // deeper tint
const LINE  = "CBDCE6";
const GREEN = "2E8B7A";
const MONO  = "Courier New";
const SERIF = "Cambria";
const SANS  = "Calibri";

let n = 0;
function foot(s, dark) {
  n++;
  s.addText("dsupdt  \u2022  Developer & General Usage Guide", {
    x:0.5, y:H-0.42, w:8, h:0.3, fontFace:SANS, fontSize:9,
    color: dark?"7C93A8":MUTE, align:"left", margin:0 });
  s.addText(String(n), {
    x:W-0.9, y:H-0.42, w:0.4, h:0.3, fontFace:SANS, fontSize:9,
    color: dark?"7C93A8":MUTE, align:"right", margin:0 });
}
function kicker(s, txt, color) {
  s.addText(txt.toUpperCase(), { x:0.5, y:0.42, w:9, h:0.3, fontFace:SANS,
    fontSize:12, bold:true, color:color||TEAL, charSpacing:2, margin:0 });
}
function title(s, txt) {
  s.addText(txt, { x:0.5, y:0.72, w:12.3, h:0.7, fontFace:SERIF,
    fontSize:32, bold:true, color:INK, margin:0 });
}
function circ(s, x, y, d, fill, glyph, gcolor, gsize) {
  s.addShape(p.ShapeType.ellipse, { x,y,w:d,h:d, fill:{color:fill},
    line:{type:"none"} });
  s.addText(glyph, { x, y, w:d, h:d, align:"center", valign:"middle",
    fontFace:SANS, fontSize:gsize||16, bold:true, color:gcolor||LIGHT, margin:0 });
}

// ============================================================ 1 TITLE
(() => {
  const s = p.addSlide(); s.background = { color: MID };
  // faint concentric motif circles (top-right)
  [3.4,2.5,1.6].forEach((d,i)=> s.addShape(p.ShapeType.ellipse,
    { x:10.2-d/2, y:1.1-d/2+0.2, w:d, h:d, fill:{type:"none"},
      line:{color:i===2?AMBER:"1C3A55", width:i===2?2:1} }));
  circ(s, 9.55, 0.9, 0.9, AMBER, "\u21BB", MID, 30);
  s.addText("dsupdt", { x:0.7, y:2.35, w:9, h:1.4, fontFace:SERIF,
    fontSize:78, bold:true, color:LIGHT, margin:0 });
  s.addText("Periodic Dataset Updates for the GDEX Servers", {
    x:0.72, y:3.7, w:11.5, h:0.6, fontFace:SANS, fontSize:24,
    color:"CADCFC", margin:0 });
  s.addText([
    { text:"Download \u2192 Build \u2192 Archive, scheduled and automated", options:{} }
  ], { x:0.72, y:4.35, w:11, h:0.5, fontFace:SANS, italic:true,
       fontSize:15, color:"8FB0C9", margin:0 });
  // meta chips
  const chips = ["Developer Configuration", "General Usage", "Updated 2026-07"];
  let cx = 0.72;
  chips.forEach(c => {
    const w = 0.28 + c.length*0.098;
    s.addShape(p.ShapeType.roundRect, { x:cx, y:5.35, w, h:0.5,
      rectRadius:0.1, fill:{color:"16273D"}, line:{color:"2A425C", width:1} });
    s.addText(c, { x:cx, y:5.35, w, h:0.5, align:"center", valign:"middle",
      fontFace:SANS, fontSize:12, bold:true, color:"CADCFC", margin:0 });
    cx += w + 0.2;
  });
  s.addText([
    { text:"Zaihua Ji", options:{ bold:true } },
    { text:"   zji@ucar.edu", options:{} },
  ], { x:0.72, y:6.05, w:11, h:0.4, fontFace:SANS, fontSize:13, color:"8FB0C9", margin:0 });
  s.addText("Companion utility to  dsarch  \u2022  orchestrated by the  dscheck  daemon", {
    x:0.72, y:6.5, w:11, h:0.4, fontFace:MONO, fontSize:12, color:"6F8CA6", margin:0 });
})();

// ============================================================ 2 OVERVIEW
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Overview", TEAL); title(s, "What is dsupdt?");
  s.addText([
    { text:"dsupdt", options:{ bold:true, color:DEEP } },
    { text:" performs ", options:{} },
    { text:"periodic dataset updates", options:{ bold:true } },
    { text:" \u2014 it retrieves data from remote servers, assembles archive-ready local files, and hands them to ", options:{} },
    { text:"dsarch", options:{ bold:true, color:DEEP } },
    { text:" for archiving onto the GDEX Servers.", options:{} },
  ], { x:0.5, y:1.65, w:5.3, h:1.7, fontFace:SANS, fontSize:16,
       color:INK, lineSpacingMultiple:1.15, margin:0, valign:"top" });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:3.55, w:5.3, h:3.05,
    rectRadius:0.1, fill:{color:TINT}, line:{color:LINE, width:1} });
  s.addText("Configured per dataset in GDEXDB", {
    x:0.75, y:3.75, w:4.9, h:0.4, fontFace:SANS, bold:true, fontSize:14,
    color:DEEP, margin:0 });
  s.addText([
    { text:"All operational datasets run under ", options:{} },
    { text:"dsupdt / dsarch", options:{ bold:true } },
    { text:"; configuration spans three record types.", options:{} },
    { text:"\nEdit records from the CLI (", options:{} },
    { text:"-GA / -SA", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:") or the web Config Editor (next slide).", options:{} },
    { text:"\nOnly the owning specialist may run an update record (override with ", options:{} },
    { text:"-MD", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:").", options:{} },
    { text:"\nInput files must be named ", options:{} },
    { text:"dNNNNNN.*", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:" to guard against the wrong dataset.", options:{} },
  ], { x:0.75, y:4.2, w:4.85, h:2.25, fontFace:SANS, fontSize:13,
       color:INK, lineSpacingMultiple:1.12, margin:0, valign:"top" });

  const caps = [
    ["\u2699","Configure","update controls, local & remote file records"],
    ["\u2193","Download","copy / fetch server files to a working area"],
    ["\u26A1","Validate & Build","run specialist routines, tar & compress"],
    ["\u2601","Archive","hand local files to dsarch \u2192 GDEX"],
    ["\u2717","Clean","remove temporary files after success"],
    ["\u2713","Check","test remote-file availability"],
  ];
  const gx0=6.25, gy0=1.65, cw=3.35, ch=1.55, gapx=0.25, gapy=0.2;
  caps.forEach((c,i)=>{
    const col=i%2, row=Math.floor(i/2);
    const x=gx0+col*(cw+gapx), y=gy0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.08,
      fill:{color:LIGHT}, line:{color:LINE, width:1},
      shadow:{type:"outer", color:"9FB6C6", blur:5, offset:2, angle:90, opacity:0.35} });
    circ(s, x+0.22, y+0.24, 0.62, i%2?TEAL:DEEP, c[0], LIGHT, 20);
    s.addText(c[1], { x:x+1.0, y:y+0.22, w:cw-1.15, h:0.4, fontFace:SANS,
      bold:true, fontSize:15, color:INK, margin:0, valign:"middle" });
    s.addText(c[2], { x:x+1.0, y:y+0.66, w:cw-1.15, h:0.78, fontFace:SANS,
      fontSize:12, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.05 });
  });
  foot(s);
})();

// ============================================================ 3 PIPELINE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "How it works", TEAL); title(s, "The Five-Stage Update Pipeline");
  const stages = [
    ["1","Server Files","original files on the remote server or a local area", DEEP],
    ["2","Remote Files","staged in the working directory (-WD)", TEAL],
    ["3","Local Files","validated, converted, tarred / compressed", GREEN],
    ["4","dsarch","archive action AS / AW / AQ applied", AMBER],
    ["5","GDEX Servers","published & discoverable", MID],
  ];
  const y=2.2, cw=2.18, ch=2.5, gap=0.35;
  let x=0.55;
  stages.forEach((st,i)=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color: i===4?MID:TINT}, line:{color: st[3], width:2} });
    circ(s, x+cw/2-0.35, y+0.28, 0.7, st[3], st[0], LIGHT, 24);
    s.addText(st[1], { x:x+0.1, y:y+1.08, w:cw-0.2, h:0.55, align:"center",
      fontFace:SANS, bold:true, fontSize:15, color: i===4?LIGHT:INK, margin:0, valign:"middle" });
    s.addText(st[2], { x:x+0.15, y:y+1.6, w:cw-0.3, h:0.8, align:"center",
      fontFace:SANS, fontSize:11.5, color: i===4?"CADCFC":MUTE, margin:0, valign:"top",
      lineSpacingMultiple:1.05 });
    if (i<4){
      const ax = x+cw+0.02;
      s.addText("\u2192", { x:ax, y, w:gap-0.04, h:ch, align:"center",
        valign:"middle", fontFace:SANS, fontSize:26, bold:true, color:st[3], margin:0 });
    }
    x += cw+gap;
  });
  s.addShape(p.ShapeType.roundRect, { x:0.55, y:5.35, w:12.25, h:1.35, rectRadius:0.1,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  circ(s, 0.8, 5.62, 0.8, DEEP, "\u2318", LIGHT, 22);
  s.addText([
    { text:"One command runs the whole chain.  ", options:{ bold:true, color:DEEP } },
    { text:"-UF (-UpdateFile)", options:{ fontFace:MONO, bold:true, color:INK } },
    { text:" performs all stages end-to-end. The individual steps ", options:{} },
    { text:"-DR", options:{ fontFace:MONO, bold:true } },
    { text:", ", options:{} },
    { text:"-BL", options:{ fontFace:MONO, bold:true } },
    { text:", ", options:{} },
    { text:"-PB", options:{ fontFace:MONO, bold:true } },
    { text:", ", options:{} },
    { text:"-AF", options:{ fontFace:MONO, bold:true } },
    { text:", ", options:{} },
    { text:"-CF", options:{ fontFace:MONO, bold:true } },
    { text:" let you drive each stage separately.", options:{} },
  ], { x:1.8, y:5.55, w:10.8, h:0.95, fontFace:SANS, fontSize:14, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.1 });
  foot(s);
})();

// ============================================================ 4 DATA MODEL
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Data model", TEAL); title(s, "Three Record Types in GDEXDB");
  const cards = [
    ["dcupdt","Update Control","-SC / -GC", DEEP,
      ["Schedules when updates run","Frequency, offset, retry, valid interval","Email & error controls","Launched by the dscheck daemon"]],
    ["dlupdt","Local File","-SL / -GL", TEAL,
      ["Minimum config for an update","Local file name & dsarch action","Download / build / clean commands","Tracks next end date/hour"]],
    ["drupdt","Remote File","-SR / -GR", GREEN,
      ["Only when remote \u2260 local name","Many remotes \u2192 one local file","Server file & download order","Time interval for sub-periods"]],
  ];
  const y=1.7, cw=3.95, ch=4.5, gap=0.28; let x=0.5;
  cards.forEach(c=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color:LIGHT}, line:{color:c[3], width:2},
      shadow:{type:"outer", color:"9FB6C6", blur:6, offset:2, angle:90, opacity:0.3} });
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:1.15, rectRadius:0.1,
      fill:{color:c[3]}, line:{type:"none"} });
    // mask bottom corners of header
    s.addShape(p.ShapeType.rect, { x, y:y+0.6, w:cw, h:0.55, fill:{color:c[3]}, line:{type:"none"} });
    s.addText(c[1], { x:x+0.25, y:y+0.14, w:cw-0.5, h:0.45, fontFace:SERIF,
      bold:true, fontSize:20, color:LIGHT, margin:0, valign:"middle" });
    s.addText([
      { text:c[0]+"   ", options:{ fontFace:MONO, fontSize:12, color:"E8F2F8" } },
      { text:c[2], options:{ fontFace:MONO, fontSize:12, bold:true, color:"FFFFFF" } },
    ], { x:x+0.25, y:y+0.62, w:cw-0.5, h:0.4, margin:0, valign:"middle" });
    s.addText(c[4].map((t,i)=>({ text:t, options:{ bullet:{code:"2022", indent:14},
      breakLine:true, paraSpaceAfter: i===c[4].length-1?0:8 } })),
      { x:x+0.3, y:y+1.4, w:cw-0.55, h:ch-1.6, fontFace:SANS, fontSize:13.5,
        color:INK, margin:0, valign:"top", lineSpacingMultiple:1.05 });
    x += cw+gap;
  });
  s.addText([
    { text:"Get \u2192 edit \u2192 Set:  ", options:{ bold:true, color:DEEP } },
    { text:"-GA (-GetAll)", options:{ fontFace:MONO, bold:true } },
    { text:" dumps all three sections;  ", options:{} },
    { text:"-SA (-SetAll)", options:{ fontFace:MONO, bold:true } },
    { text:" applies an edited file back to GDEXDB.", options:{} },
  ], { x:0.5, y:6.45, w:12.3, h:0.4, fontFace:SANS, fontSize:13, color:MUTE,
       align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 5 COMMAND ANATOMY
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "General usage", TEAL); title(s, "Anatomy of a Command");
  // command line render
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:1.65, w:12.3, h:0.95,
    rectRadius:0.08, fill:{color:MID}, line:{type:"none"} });
  s.addText([
    { text:"dsupdt ", options:{ color:LIGHT } },
    { text:"[-DS dNNNNNN] ", options:{ color:"8FB0C9" } },
    { text:"<Action> ", options:{ color:AMBER, bold:true } },
    { text:"[Mode\u2026] ", options:{ color:"7FD1B9", bold:true } },
    { text:"[Info\u2026]", options:{ color:"9FC4E0", bold:true } },
  ], { x:0.75, y:1.65, w:11.8, h:0.95, fontFace:MONO, fontSize:22, margin:0, valign:"middle" });

  const blocks = [
    ["Action","One per run","Selects the task. Takes no value. E.g. -UF, -GC, -SL, -AF. Some actions imply others.", AMBER, "9C6416"],
    ["Mode","Zero or more","Modifies behavior; takes no value. E.g. -MU, -FU, -RA, -NE, -GZ.", GREEN, "1F5E4F"],
    ["Info","Carries data","Supplies values: dataset, indices, dates, file names, commands. E.g. -CI, -ED, -LF.", TEAL, "0F4A61"],
  ];
  const y=2.95, cw=3.95, ch=2.35, gap=0.28; let x=0.5;
  blocks.forEach(b=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color:TINT}, line:{color:b[3], width:2} });
    s.addText(b[0], { x:x+0.28, y:y+0.22, w:cw-1.6, h:0.5, fontFace:SERIF,
      bold:true, fontSize:22, color:b[4], margin:0 });
    s.addShape(p.ShapeType.roundRect, { x:x+cw-1.55, y:y+0.25, w:1.3, h:0.42,
      rectRadius:0.08, fill:{color:b[3]}, line:{type:"none"} });
    s.addText(b[1], { x:x+cw-1.55, y:y+0.25, w:1.3, h:0.42, align:"center",
      valign:"middle", fontFace:SANS, bold:true, fontSize:10.5, color:LIGHT, margin:0 });
    s.addText(b[2], { x:x+0.28, y:y+0.9, w:cw-0.55, h:1.3, fontFace:SANS,
      fontSize:13, color:INK, margin:0, valign:"top", lineSpacingMultiple:1.1 });
    x += cw+gap;
  });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.55, w:12.3, h:1.1, rectRadius:0.1,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Notation & filters   ", options:{ bold:true, color:DEEP } },
    { text:"(A|B)", options:{ fontFace:MONO, bold:true } },
    { text:" short|long form  \u2022  option names are case-insensitive, values are not  \u2022  Get-style filters accept  ", options:{} },
    { text:"! < > <>", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:"  \u2022  the first  ", options:{} },
    { text:"-DS", options:{ fontFace:MONO, bold:true } },
    { text:"  may omit its option name.", options:{} },
  ], { x:0.8, y:5.62, w:11.8, h:0.95, fontFace:SANS, fontSize:13.5, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.1 });
  foot(s);
})();

// ============================================================ 6 QUICK START
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "General usage", TEAL); title(s, "Quick Start");
  const cmds = [
    ["Show a single option's help","dsupdt -h -UF", DEEP],
    ["Get update control records","dsupdt d540000 -GC", TEAL],
    ["Get local file records","dsupdt d540000 -GL", TEAL],
    ["Run download\u2013build\u2013archive","dsupdt d540000 -UF", GREEN],
    ["Process all elapsed periods","dsupdt d540000 -UF -MU -IE", GREEN],
    ["Check remote-file availability","dsupdt d540000 -CU", AMBER],
  ];
  const y0=1.7, cw=6.0, ch=1.42, gapx=0.3, gapy=0.22; let i=0;
  cmds.forEach(c=>{
    const col=i%2, row=Math.floor(i/2);
    const x=0.5+col*(cw+gapx), y=y0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.08,
      fill:{color:LIGHT}, line:{color:LINE, width:1},
      shadow:{type:"outer", color:"9FB6C6", blur:5, offset:2, angle:90, opacity:0.3} });
    s.addShape(p.ShapeType.ellipse, { x:x+0.28, y:y+0.5, w:0.42, h:0.42,
      fill:{color:c[2]}, line:{type:"none"} });
    s.addText("\u203A", { x:x+0.28, y:y+0.47, w:0.42, h:0.42, align:"center",
      valign:"middle", fontFace:SANS, bold:true, fontSize:18, color:LIGHT, margin:0 });
    s.addText(c[0], { x:x+0.9, y:y+0.2, w:cw-1.1, h:0.4, fontFace:SANS,
      bold:true, fontSize:14, color:INK, margin:0, valign:"middle" });
    s.addShape(p.ShapeType.roundRect, { x:x+0.9, y:y+0.62, w:cw-1.15, h:0.55,
      rectRadius:0.05, fill:{color:MID}, line:{type:"none"} });
    s.addText(c[1], { x:x+1.05, y:y+0.62, w:cw-1.4, h:0.55, fontFace:MONO,
      fontSize:13, color:"9FE0C9", margin:0, valign:"middle" });
    i++;
  });
  s.addText([
    { text:"Tip:  ", options:{ bold:true, color:AMBER } },
    { text:"run  ", options:{} },
    { text:"dsupdt", options:{ fontFace:MONO, bold:true } },
    { text:"  with no arguments to page the full guide through  ", options:{} },
    { text:"more", options:{ fontFace:MONO, bold:true } },
    { text:".  Omit  ", options:{} },
    { text:"-DS", options:{ fontFace:MONO, bold:true } },
    { text:"  on Get actions to list records across all of your datasets.", options:{} },
  ], { x:0.5, y:6.5, w:12.3, h:0.4, fontFace:SANS, fontSize:13, color:INK,
       align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 7 CONFIG WORKFLOW
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Developer configuration", DEEP); title(s, "Configuration Workflow");
  // three-step loop
  const steps = [
    ["\u2193","GET","dsupdt d277000 -GA -OF d277000.all","Dump all control/local/remote records to a file", DEEP],
    ["\u270E","EDIT","vi d277000.all","Adjust values; sections [DCUPDT] [DLUPDT] [DRUPDT]", AMBER],
    ["\u2191","SET","dsupdt d277000 -SA -IF d277000.all","Apply the edited file back to GDEXDB", GREEN],
  ];
  const y=1.7, cw=3.95, ch=2.55, gap=0.28; let x=0.5;
  steps.forEach((st,i)=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color:TINT}, line:{color:st[4], width:2} });
    circ(s, x+0.28, y+0.28, 0.7, st[4], st[0], LIGHT, 22);
    s.addText(st[1], { x:x+1.1, y:y+0.3, w:cw-1.3, h:0.6, fontFace:SERIF,
      bold:true, fontSize:24, color:st[4], margin:0, valign:"middle" });
    s.addShape(p.ShapeType.roundRect, { x:x+0.25, y:y+1.15, w:cw-0.5, h:0.62,
      rectRadius:0.05, fill:{color:MID}, line:{type:"none"} });
    s.addText(st[2], { x:x+0.38, y:y+1.15, w:cw-0.72, h:0.62, fontFace:MONO,
      fontSize:11, color:"9FE0C9", margin:0, valign:"middle" });
    s.addText(st[3], { x:x+0.28, y:y+1.85, w:cw-0.5, h:0.65, fontFace:SANS,
      fontSize:12.5, color:INK, margin:0, valign:"top", lineSpacingMultiple:1.05 });
    if (i<2) s.addText("\u2192", { x:x+cw-0.05, y:y+0.6, w:gap+0.1, h:0.7,
      align:"center", valign:"middle", fontFace:SANS, fontSize:24, bold:true,
      color:MUTE, margin:0 });
    x += cw+gap;
  });
  // input file format
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:4.55, w:12.3, h:2.1, rectRadius:0.1,
    fill:{color:LIGHT}, line:{color:LINE, width:1} });
  s.addText("Input file (-IF) format", { x:0.75, y:4.7, w:6, h:0.4,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  const fmt = [
    ["# ...","comment line"],
    ["Dataset<=>d277000","single-value assignment  ( -ES sets '<=>' )"],
    ["SC<!>","Action / Mode marker  ( -AO sets '<!>' )"],
    ["col1<:>col2<:>","tabular header row  ( -DV sets '<:>' )"],
    ["dNNNNNN.*","file name must start with the dataset number"],
  ];
  let fy=5.15;
  fmt.forEach(f=>{
    s.addText(f[0], { x:0.9, y:fy, w:3.4, h:0.3, fontFace:MONO, fontSize:12.5,
      bold:true, color:INK, margin:0, valign:"middle" });
    s.addText(f[1], { x:4.5, y:fy, w:8.1, h:0.3, fontFace:SANS, fontSize:12.5,
      color:MUTE, margin:0, valign:"middle" });
    fy += 0.3;
  });
  foot(s);
})();

// ============================================================ 8 WEB CONFIG EDITOR
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Developer configuration", DEEP); title(s, "Web Config Editor");
  s.addText([
    { text:"The same three record types can be edited in a browser \u2014 no ", options:{} },
    { text:"-GA / -SA", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:" Get/Edit/Set round-trip. Open the ", options:{} },
    { text:"RDAMS PostgreSQL Configuration Editor", options:{ bold:true } },
    { text:" and expand the ", options:{} },
    { text:"DSUPDT", options:{ bold:true, color:DEEP } },
    { text:" menu.", options:{} },
  ], { x:0.5, y:1.6, w:12.3, h:0.6, fontFace:SANS, fontSize:15,
       color:INK, lineSpacingMultiple:1.1, margin:0, valign:"top" });
  // URL bar
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:2.35, w:12.33, h:0.55, rectRadius:0.06,
    fill:{color:MID} });
  s.addText([
    { text:"gdex.ucar.edu/rda_pg_config", options:{ color:"9FE0C9" } },
    { text:"      \u203A  DSUPDT  \u203A  Update Control | Local File | Remote File", options:{ color:AMBER } },
  ], { x:0.78, y:2.35, w:12, h:0.55, fontFace:MONO, fontSize:13, valign:"middle", margin:0 });
  // three subsection cards mirroring the CLI record types
  const subs = [
    ["Update Control","dsupdt_control.php","dcupdt \u2022 -SC / -GC","frequency, next-due, actions, e-mail & error controls", DEEP],
    ["Local File","dsupdt_localfile.php","dlupdt \u2022 -SL / -GL","file names, patterns, build / convert commands, archive action", TEAL],
    ["Remote File","dsupdt_remotefile.php","drupdt \u2022 -SR / -GR","remote & server names, download command & order", GREEN],
  ];
  const y=3.2, cw=3.95, ch=2.35, gap=0.28; let x=0.5;
  subs.forEach(c=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color:TINT}, line:{color:c[4], width:2} });
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:0.6, rectRadius:0.1,
      fill:{color:c[4]}, line:{type:"none"} });
    s.addShape(p.ShapeType.rect, { x, y:y+0.38, w:cw, h:0.22, fill:{color:c[4]}, line:{type:"none"} });
    s.addText(c[0], { x:x+0.22, y, w:cw-0.44, h:0.6, fontFace:SERIF, bold:true,
      fontSize:17, color:LIGHT, margin:0, valign:"middle" });
    s.addText(c[1], { x:x+0.25, y:y+0.76, w:cw-0.5, h:0.32, fontFace:MONO,
      fontSize:11, color:MUTE, margin:0, valign:"middle" });
    s.addText(c[2], { x:x+0.25, y:y+1.1, w:cw-0.5, h:0.32, fontFace:MONO,
      bold:true, fontSize:11.5, color:c[4], margin:0, valign:"middle" });
    s.addText(c[3], { x:x+0.25, y:y+1.48, w:cw-0.5, h:0.75, fontFace:SANS,
      fontSize:12, color:INK, margin:0, valign:"top", lineSpacingMultiple:1.05 });
    x += cw+gap;
  });
  // editor UX note strip
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.9, w:12.33, h:0.9, rectRadius:0.1,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  circ(s, 0.75, 6.15, 0.5, DEEP, "\u270E", LIGHT, 16);
  s.addText([
    { text:"Split-pane editor: ", options:{ bold:true, color:DEEP } },
    { text:"pick a dataset & record on top, edit fields below. A record is ", options:{} },
    { text:"locked", options:{ bold:true } },
    { text:" while open; add, edit, save, or delete, then release. Writes go straight to GDEXDB \u2014 identical to the CLI.", options:{} },
  ], { x:1.4, y:5.9, w:11.2, h:0.9, fontFace:SANS, fontSize:12.5,
       color:INK, valign:"middle", margin:0, lineSpacingMultiple:1.05 });
  foot(s);
})();

// ============================================================ 9 UPDATE CONTROL -SC
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Developer configuration \u2022 dcupdt", DEEP);
  title(s, "Update Control Record  (-SC)");
  s.addText([
    { text:"Schedules automatic actions launched by ", options:{} },
    { text:"dscheck", options:{ bold:true, color:DEEP } },
    { text:".  New record: index ", options:{} },
    { text:"0", options:{ fontFace:MONO, bold:true } },
    { text:" + ", options:{} },
    { text:"-NC", options:{ fontFace:MONO, bold:true } },
    { text:".  Existing index is modified in place.", options:{} },
  ], { x:0.5, y:1.55, w:12.3, h:0.4, fontFace:SANS, fontSize:14, color:INK, margin:0 });

  const rows = [
    ["-AN","ActionName","dsupdt task: UF, DR, BL, PB, CL, CU"],
    ["-FQ","Frequency","how often, e.g. 1W, 1M, 6H, 1M/3"],
    ["-CO","ControlOffset","delay after period end, e.g. 2D10H"],
    ["-CT","ControlTime","next scheduled run  YYYY-MM-DD HH:NN:SS"],
    ["-RI","RetryInterval","wait before retry after a failure"],
    ["-VI","ValidInterval","how long remote files stay valid"],
    ["-PI","ParentIndex","run only after parent control completes"],
    ["-UC","UpdateControl","preset modes: B C E F G M N O Y Z"],
    ["-MC","EMailControl","A / S / E / B / N report level"],
    ["-EC","ErrorControl","I ignore / Q quit / N normal"],
    ["-KF","KeepFile","S / R / B / N which files to keep"],
    ["-HN","HostName","restrict/allow processing hosts"],
  ];
  const tblRows = [[
    { text:"Opt", options:{ bold:true, color:LIGHT, fill:{color:DEEP}, fontFace:MONO } },
    { text:"Long name", options:{ bold:true, color:LIGHT, fill:{color:DEEP} } },
    { text:"Purpose", options:{ bold:true, color:LIGHT, fill:{color:DEEP} } },
  ]];
  rows.forEach((r,i)=>{
    const bg = i%2 ? TINT : LIGHT;
    tblRows.push([
      { text:r[0], options:{ fontFace:MONO, bold:true, color:DEEP, fill:{color:bg} } },
      { text:r[1], options:{ color:INK, fill:{color:bg} } },
      { text:r[2], options:{ color:MUTE, fill:{color:bg} } },
    ]);
  });
  s.addTable(tblRows, { x:0.5, y:2.1, w:12.33, colW:[1.1,2.6,8.63],
    rowH:0.36, fontFace:SANS, fontSize:12.5, valign:"middle",
    border:{type:"solid", color:LINE, pt:0.5}, margin:[0,0.08,0,0.08] });
  foot(s);
})();

// ============================================================ 9 LOCAL FILE -SL
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Developer configuration \u2022 dlupdt", TEAL);
  title(s, "Local File Record  (-SL)");
  s.addText([
    { text:"The ", options:{} },
    { text:"minimum", options:{ bold:true } },
    { text:" configuration to update a file. Link to a control via ", options:{} },
    { text:"-CI", options:{ fontFace:MONO, bold:true, color:TEAL } },
    { text:" so its scheduled action runs automatically.  New: ", options:{} },
    { text:"index 0 + -NL", options:{ fontFace:MONO, bold:true } },
    { text:".", options:{} },
  ], { x:0.5, y:1.55, w:12.3, h:0.4, fontFace:SANS, fontSize:14, color:INK, margin:0 });

  const left = [
    ["-LF","LocalFile","name, may embed temporal patterns"],
    ["-AN","ActionName","dsarch action: AS, AW, AQ"],
    ["-OP","Options","options string passed to dsarch"],
    ["-DC","DownloadCommand","fetch/copy/generate the file"],
    ["-FQ","Frequency","data cadence, e.g. 1M, 1W, 6H"],
    ["-ED","EndDate","data end date of next update"],
    ["-EH","EndHour","end hour (sub-daily cadence)"],
  ];
  const right = [
    ["-DI","DueInterval","end \u2192 ready lead time"],
    ["-VI","ValidInterval","re-check / hold-open window"],
    ["-AT","AgeTime","min remote-file age to fetch"],
    ["-WD","WorkDir","staging dir (root $UPDTWKP)"],
    ["-PR","ProcessRemote","user validate/convert cmd"],
    ["-BC","BuildCommand","user build cmd (vs tar)"],
    ["-CL","CleanCommand","remove temp files after"],
  ];
  function tbl(data, x){
    const rr=[[
      { text:"Opt", options:{ bold:true, color:LIGHT, fill:{color:TEAL}, fontFace:MONO } },
      { text:"Long name", options:{ bold:true, color:LIGHT, fill:{color:TEAL} } },
      { text:"Purpose", options:{ bold:true, color:LIGHT, fill:{color:TEAL} } },
    ]];
    data.forEach((r,i)=>{ const bg=i%2?TINT:LIGHT; rr.push([
      { text:r[0], options:{ fontFace:MONO, bold:true, color:TEAL, fill:{color:bg} } },
      { text:r[1], options:{ color:INK, fill:{color:bg} } },
      { text:r[2], options:{ color:MUTE, fill:{color:bg} } },
    ]); });
    s.addTable(rr, { x, y:2.1, w:6.05, colW:[0.95,1.85,3.25], rowH:0.42,
      fontFace:SANS, fontSize:11.5, valign:"middle",
      border:{type:"solid", color:LINE, pt:0.5}, margin:[0,0.06,0,0.06] });
  }
  tbl(left, 0.5); tbl(right, 6.78);
  s.addText([
    { text:"Patterns  ", options:{ bold:true, color:AMBER } },
    { text:"in -LF / -RF / -DC / -DE (default delimiters ", options:{} },
    { text:"< >", options:{ fontFace:MONO, bold:true } },
    { text:") are substituted at update time, e.g. ", options:{} },
    { text:"file.<YYYYMM>.ext \u2192 file.200710.ext", options:{ fontFace:MONO, bold:true, color:INK } },
    { text:".", options:{} },
  ], { x:0.5, y:5.35, w:12.3, h:0.4, fontFace:SANS, fontSize:12.5, color:MUTE,
       align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 10 REMOTE FILE -SR
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Developer configuration \u2022 drupdt", GREEN);
  title(s, "Remote File Record  (-SR)");
  s.addText("Add a remote record only in these three cases:", {
    x:0.5, y:1.55, w:12, h:0.35, fontFace:SANS, bold:true, fontSize:15, color:INK, margin:0 });
  const cases = [
    ["\u2260","Different name","remote file name differs from the local file name"],
    ["\u2211","Many \u2192 one","several remote files are tarred into one local file"],
    ["\u21C4","Multiple servers","one file available from several servers (-DO order)"],
  ];
  const y=2.05, cw=3.95, ch=1.55, gap=0.28; let x=0.5;
  cases.forEach(c=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color:TINT}, line:{color:GREEN, width:1.5} });
    circ(s, x+0.24, y+0.44, 0.66, GREEN, c[0], LIGHT, 22);
    s.addText(c[1], { x:x+1.02, y:y+0.2, w:cw-1.2, h:0.4, fontFace:SANS,
      bold:true, fontSize:15, color:INK, margin:0, valign:"middle" });
    s.addText(c[2], { x:x+1.02, y:y+0.62, w:cw-1.2, h:0.8, fontFace:SANS,
      fontSize:12, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.05 });
    x += cw+gap;
  });
  const rows = [
    ["-LI","LocalIndex","local record this remote belongs to (required)"],
    ["-RF","RemoteFile","remote name(s); join many with '::' ; '!' = shell cmd"],
    ["-SF","ServerFile","name on server if different from remote name"],
    ["-DC","DownloadCommand","overrides the local record's command"],
    ["-DO","DownloadOrder","try lowest index first, then ascending"],
    ["-TI","TimeInterval","one name per sub-period, e.g. 6H, 5D"],
    ["-BT","BeginTime","window start (with -TI)"],
    ["-ET","EndTime","window end (with -TI)"],
  ];
  const rr=[[
    { text:"Opt", options:{ bold:true, color:LIGHT, fill:{color:GREEN}, fontFace:MONO } },
    { text:"Long name", options:{ bold:true, color:LIGHT, fill:{color:GREEN} } },
    { text:"Purpose", options:{ bold:true, color:LIGHT, fill:{color:GREEN} } },
  ]];
  rows.forEach((r,i)=>{ const bg=i%2?TINT:LIGHT; rr.push([
    { text:r[0], options:{ fontFace:MONO, bold:true, color:GREEN, fill:{color:bg} } },
    { text:r[1], options:{ color:INK, fill:{color:bg} } },
    { text:r[2], options:{ color:MUTE, fill:{color:bg} } },
  ]); });
  s.addTable(rr, { x:0.5, y:3.8, w:12.33, colW:[1.1,2.5,8.73], rowH:0.33,
    fontFace:SANS, fontSize:12.5, valign:"middle",
    border:{type:"solid", color:LINE, pt:0.5}, margin:[0,0.08,0,0.08] });
  foot(s);
})();

// ============================================================ 11 UPDATE ACTIONS FAMILY
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Update actions", AMBER); title(s, "The Update Action Family");
  const acts = [
    ["-UF","Update File","end-to-end: download \u2192 build \u2192 archive \u2192 clean", AMBER, true],
    ["-DR","Download Remote","fetch/copy/generate remote files", DEEP],
    ["-BL","Build Local","validate, convert, tar/compress", TEAL],
    ["-PB","Process Both","download + build in one step", GREEN],
    ["-AF","Archive File","hand local files to dsarch", DEEP],
    ["-CF","Clean File","remove temporary working files", TEAL],
    ["-CU","Check Update","test remote-file availability", GREEN],
    ["-UL","Unlock","clear locks from aborted runs", MUTE],
  ];
  const y0=1.7, cw=3.0, ch=1.5, gapx=0.12, gapy=0.2; let i=0;
  acts.forEach(a=>{
    const col=i%4, row=Math.floor(i/4);
    const x=0.5+col*(cw+gapx), y=y0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.08,
      fill:{color: a[4]?a[3]:LIGHT}, line:{color:a[3], width: a[4]?0:1.5},
      shadow:{type:"outer", color:"9FB6C6", blur:4, offset:2, angle:90, opacity:0.25} });
    s.addText(a[0], { x:x+0.2, y:y+0.15, w:cw-0.4, h:0.42, fontFace:MONO,
      bold:true, fontSize:20, color: a[4]?LIGHT:a[3], margin:0 });
    s.addText(a[1], { x:x+0.2, y:y+0.55, w:cw-0.4, h:0.35, fontFace:SANS,
      bold:true, fontSize:13.5, color: a[4]?LIGHT:INK, margin:0 });
    s.addText(a[2], { x:x+0.2, y:y+0.9, w:cw-0.35, h:0.55, fontFace:SANS,
      fontSize:10.5, color: a[4]?"F0E4D4":MUTE, margin:0, valign:"top",
      lineSpacingMultiple:1.02 });
    i++;
  });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.35, w:12.33, h:1.3, rectRadius:0.1,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"-UF bundles the four steps.  ", options:{ bold:true, color:DEEP } },
    { text:"Stepping manually is useful for debugging or re-running a single stage. All actions accept ", options:{} },
    { text:"-CI / -LI / -LF / -XO", options:{ fontFace:MONO, bold:true } },
    { text:" to target records, and ", options:{} },
    { text:"-ED / -EH / -CD / -CH", options:{ fontFace:MONO, bold:true } },
    { text:" to override dates at run time.", options:{} },
  ], { x:0.8, y:5.5, w:11.8, h:1.0, fontFace:SANS, fontSize:13.5, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.12 });
  foot(s);
})();

// ============================================================ 12 -UF DEEP DIVE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Update actions", AMBER); title(s, "-UF Deep Dive: One Command, Full Cycle");
  // left: what it does list
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:1.7, w:6.0, h:4.9, rectRadius:0.1,
    fill:{color:TINT}, line:{color:LINE, width:1} });
  s.addText("For each due local file record", { x:0.75, y:1.9, w:5.5, h:0.4,
    fontFace:SANS, bold:true, fontSize:15, color:DEEP, margin:0 });
  const steps = [
    "Download / copy the server file",
    "Validate & convert (-PR) into a remote file",
    "Build the local file (tar / compress or -BC)",
    "Archive via dsarch using the record's action",
    "Clean temporary files (-CL)",
    "Advance data end date/hour & next-due times",
  ];
  s.addText(steps.map((t,i)=>({ text:t, options:{ bullet:{type:"number", indent:20},
    breakLine:true, paraSpaceAfter: i===steps.length-1?0:9 } })),
    { x:0.9, y:2.4, w:5.4, h:4.0, fontFace:SANS, fontSize:14, color:INK,
      margin:0, valign:"top", lineSpacingMultiple:1.1 });

  // right: key modes
  const modes = [
    ["-MU","process every elapsed period (else newest only)"],
    ["-FU","force at least one update even if not due"],
    ["-CP","allow the current, not-yet-complete period"],
    ["-RA","re-archive (pass -RA to dsarch)"],
    ["-RD","re-download even if the file exists locally"],
    ["-MO","only periods not yet archived"],
    ["-IE","skip failures & continue (with -MU)"],
    ["-QE","stop the dataset on first error"],
    ["-PL","fork N child processes for parallel records"],
    ["-CC","add carbon-copy email recipients"],
  ];
  const rr=[[
    { text:"Mode/Info", options:{ bold:true, color:LIGHT, fill:{color:AMBER}, fontFace:MONO } },
    { text:"Effect", options:{ bold:true, color:LIGHT, fill:{color:AMBER} } },
  ]];
  modes.forEach((m,i)=>{ const bg=i%2?TINT:LIGHT; rr.push([
    { text:m[0], options:{ fontFace:MONO, bold:true, color:"9C6416", fill:{color:bg} } },
    { text:m[1], options:{ color:INK, fill:{color:bg} } },
  ]); });
  s.addTable(rr, { x:6.78, y:1.7, w:6.05, colW:[1.5,4.55], rowH:0.42,
    fontFace:SANS, fontSize:12, valign:"middle",
    border:{type:"solid", color:LINE, pt:0.5}, margin:[0,0.07,0,0.07] });
  s.addShape(p.ShapeType.roundRect, { x:6.78, y:6.5, w:6.05, h:0.42, rectRadius:0.06,
    fill:{color:INK} });
  s.addText("dsupdt d337000 -UF -MU -IE -CC schuster", { x:6.98, y:6.5, w:5.65, h:0.42,
    fontFace:MONO, fontSize:12, color:"9FE0C9", valign:"middle", margin:0 });
  foot(s);
})();

// ============================================================ 13A ARCHIVING & METADATA
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "How it works", TEAL); title(s, "Archiving & Content Metadata");
  s.addText([
    { text:"Once a local file is built, ", options:{} },
    { text:"dsupdt", options:{ bold:true, color:DEEP } },
    { text:" hands it to ", options:{} },
    { text:"dsarch", options:{ bold:true, color:DEEP } },
    { text:" to archive onto GDEX, then optionally gathers content metadata so the new data is discoverable.", options:{} },
  ], { x:0.5, y:1.6, w:12.3, h:0.5, fontFace:SANS, fontSize:15,
       color:INK, margin:0, valign:"top", lineSpacingMultiple:1.1 });
  const rows = [
    ["\u2601","Archive to GDEX  \u2014  dsarch","dsupdt calls dsarch with the record's action (AS / AW / AQ). -OP appends extra dsarch options; -RA forces re-archiving. This is the step that publishes each file.", DEEP],
    ["\u2699","Gather content metadata  \u2014  gatherxml  (-GX)","Optional. When -GX is set in the local record's Options, dsupdt runs gatherxml on the just-archived file to index its variables and spatial / temporal coverage.", GREEN],
    ["\u21BB","Refresh the search index  \u2014  scm","After archiving, dsupdt runs the search-metadata command (scm -d DS -r w) to summarize / refresh the dataset's web tables so the new content is searchable.", AMBER],
  ];
  const y0=2.35, cw=12.33, ch=1.2, gap=0.15; let i=0;
  rows.forEach(r=>{
    const y=y0+i*(ch+gap);
    s.addShape(p.ShapeType.roundRect, { x:0.5, y, w:cw, h:ch, rectRadius:0.08,
      fill:{color:TINT}, line:{color:LINE, width:1} });
    circ(s, 0.8, y+0.3, 0.6, r[3], r[0], LIGHT, 20);
    s.addText(r[1], { x:1.65, y:y+0.16, w:cw-1.4, h:0.4, fontFace:SANS,
      bold:true, fontSize:15.5, color:INK, margin:0, valign:"middle" });
    s.addText(r[2], { x:1.65, y:y+0.56, w:cw-1.55, h:0.55, fontFace:SANS,
      fontSize:12.5, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.05 });
    i++;
  });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:6.45, w:12.33, h:0.55, rectRadius:0.08,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Metadata steps are non-fatal: ", options:{ bold:true, color:DEEP } },
    { text:"a gatherxml or scm failure is reported in the e-mail but never blocks or retries the archive.", options:{} },
  ], { x:0.75, y:6.45, w:11.8, h:0.55, fontFace:SANS, fontSize:12.5,
       color:INK, margin:0, valign:"middle" });
  foot(s);
})();

// ============================================================ 13 PATTERNS
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Developer configuration", DEEP); title(s, "Temporal & Generic Patterns");
  s.addText([
    { text:"Placeholders in file names, download commands, and descriptions are substituted at update time. Default delimiters ", options:{} },
    { text:"< >", options:{ fontFace:MONO, bold:true } },
    { text:" (change with -PD).  By default patterns use the period's ", options:{} },
    { text:"end", options:{ bold:true } },
    { text:" date/hour.", options:{} },
  ], { x:0.5, y:1.55, w:12.3, h:0.6, fontFace:SANS, fontSize:14, color:INK,
       margin:0, lineSpacingMultiple:1.1, valign:"top" });
  const rows = [
    ["<YYYYMM>","end date","4-digit year + 2-digit month \u2192 200710"],
    ["<YYYY/MM/DD>","end date","path-style date components"],
    ["<CYYYY.MM.DDC>","current","wrap in C \u2026 C to use the current date/hour"],
    ["<BYYYY.MM.DDB>","begin","wrap in B \u2026 B to use the period's begin time"],
    ["<M*M>","fraction","per-fraction id: C=A,B,C  c=a,b,c  N=1,2,3"],
    ["<HH>","end hour","2-digit hour for sub-daily cadence"],
    ["<P0> <P1>","runtime","generic values supplied via -GP"],
  ];
  const rr=[[
    { text:"Pattern", options:{ bold:true, color:LIGHT, fill:{color:DEEP}, fontFace:MONO } },
    { text:"Basis", options:{ bold:true, color:LIGHT, fill:{color:DEEP} } },
    { text:"Meaning", options:{ bold:true, color:LIGHT, fill:{color:DEEP} } },
  ]];
  rows.forEach((r,i)=>{ const bg=i%2?TINT:LIGHT; rr.push([
    { text:r[0], options:{ fontFace:MONO, bold:true, color:DEEP, fill:{color:bg} } },
    { text:r[1], options:{ color:TEAL, bold:true, fill:{color:bg} } },
    { text:r[2], options:{ color:INK, fill:{color:bg} } },
  ]); });
  s.addTable(rr, { x:0.5, y:2.3, w:8.1, colW:[2.5,1.5,4.1], rowH:0.44,
    fontFace:SANS, fontSize:12.5, valign:"middle",
    border:{type:"solid", color:LINE, pt:0.5}, margin:[0,0.08,0,0.08] });

  s.addShape(p.ShapeType.roundRect, { x:8.85, y:2.3, w:3.98, h:3.55, rectRadius:0.1,
    fill:{color:MID}, line:{type:"none"} });
  s.addText("Example", { x:9.1, y:2.45, w:3.5, h:0.35, fontFace:SANS, bold:true,
    fontSize:14, color:AMBER, margin:0 });
  s.addText([
    { text:"-LF ", options:{ color:"9FC4E0" } },
    { text:"uv.<YYYYMM>.bln\n\n", options:{ color:"9FE0C9" } },
    { text:"end date ", options:{ color:"9FC4E0" } },
    { text:"2007-10-31\n\n", options:{ color:"FFFFFF" } },
    { text:"\u2193 becomes\n\n", options:{ color:AMBER, bold:true } },
    { text:"uv.200710.bln", options:{ color:"9FE0C9", bold:true } },
  ], { x:9.1, y:2.95, w:3.55, h:2.8, fontFace:MONO, fontSize:14, margin:0,
       valign:"top", lineSpacingMultiple:1.0 });

  s.addShape(p.ShapeType.roundRect, { x:0.5, y:6.0, w:12.33, h:0.7, rectRadius:0.08,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Dynamic work dir:  ", options:{ bold:true, color:DEEP } },
    { text:"when a server path carries a date the remote name lacks, embed that date in -WD so different dates don't overwrite each other.", options:{} },
  ], { x:0.75, y:6.05, w:11.8, h:0.6, fontFace:SANS, fontSize:12.5, color:INK,
       margin:0, valign:"middle" });
  foot(s);
})();

// ============================================================ 13B CUSTOM COMMANDS
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Developer configuration", DEEP); title(s, "Custom Commands with  '!'");
  s.addText([
    { text:"Some fields are shell commands by design ", options:{} },
    { text:"(-DC / -BC / -CL)", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:".  Any value field can also become a command by prefixing it with ", options:{} },
    { text:"'!'", options:{ fontFace:MONO, bold:true, color:AMBER } },
    { text:" \u2014 dsupdt runs it and uses the stdout as the value.  Temporal patterns are substituted first.", options:{} },
  ], { x:0.5, y:1.55, w:12.3, h:0.7, fontFace:SANS, fontSize:14, color:INK,
       margin:0, valign:"top", lineSpacingMultiple:1.12 });
  const cards = [
    ["-DC","Download Command","How each remote file is fetched from the server (wget, cp, mftp, \u2026).", DEEP],
    ["-BC","Build Command","External utility that assembles / converts the local file after download.", DEEP],
    ["-CL","Clean Command","Post-archive cleanup command for temporary or intermediate files.", DEEP],
    ["-RF  '!\u2026'","Remote File","Name resolved at run time \u2014 e.g. pick the newest matching server file.", AMBER],
    ["-LF  '!\u2026'","Local File","Compute the local file name from a script instead of a fixed pattern.", AMBER],
    ["-DE  '!\u2026'","Description / note","Generate a data note dynamically from command output.", AMBER],
  ];
  const y0=2.55, cw=6.05, ch=0.86, gapx=0.23, gapy=0.15;
  cards.forEach((c,i)=>{
    const col=i%2, row=Math.floor(i/2);
    const x=0.5+col*(cw+gapx), y=y0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.08,
      fill:{color:TINT}, line:{color:LINE, width:1} });
    s.addShape(p.ShapeType.roundRect, { x:x+0.18, y:y+0.19, w:1.55, h:0.48, rectRadius:0.06,
      fill:{color:c[3]} });
    s.addText(c[0], { x:x+0.18, y:y+0.19, w:1.55, h:0.48, fontFace:MONO, bold:true,
      fontSize:13, color:LIGHT, align:"center", valign:"middle", margin:0 });
    s.addText(c[1], { x:x+1.9, y:y+0.12, w:cw-2.05, h:0.32, fontFace:SANS, bold:true,
      fontSize:13.5, color:INK, margin:0, valign:"middle" });
    s.addText(c[2], { x:x+1.9, y:y+0.42, w:cw-2.05, h:0.4, fontFace:SANS,
      fontSize:11.5, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.02 });
  });
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.7, w:12.33, h:1.28, rectRadius:0.1,
    fill:{color:MID}, line:{type:"none"} });
  s.addText("Example  \u2014  pick the newest matching file at update time", { x:0.75, y:5.85,
    w:12, h:0.35, fontFace:SANS, bold:true, fontSize:13, color:AMBER, margin:0 });
  s.addText([
    { text:"dsupdt ", options:{ color:"9FC4E0" } },
    { text:"d540000 SR -LI 1 ", options:{ color:"FFFFFF" } },
    { text:"-RF ", options:{ color:"9FC4E0" } },
    { text:"'!ls -t /glade/incoming/<YYYYMM>*.grib | head -1'", options:{ color:"9FE0C9" } },
  ], { x:0.75, y:6.28, w:12, h:0.6, fontFace:MONO, fontSize:13.5, margin:0,
       valign:"middle", lineSpacingMultiple:1.0 });
  foot(s);
})();

// ============================================================ 14 SCHEDULING
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Execution", TEAL); title(s, "Scheduling & Execution");
  s.addText([
    { text:"In production the whole chain is automated \u2014 currently under ", options:{} },
    { text:"PBS batch control", options:{ bold:true, color:AMBER } },
    { text:":", options:{} },
  ], { x:0.5, y:1.55, w:12.3, h:0.4, fontFace:SANS, fontSize:15,
       color:INK, margin:0, valign:"top" });
  const cards = [
    ["1","Update Control Record",
      "Holds the schedule \u2014 frequency (-FQ), due interval (-DI), next-due time; local records link to it via -CI. A cron / daemon dsupdt run in delayed mode (-BP) records each due command in GDEXDB.",
      "generates a dscheck record", DEEP],
    ["2","dscheck daemon / cron",
      "Polls GDEXDB, dispatches each recorded command, and retries on failure \u2014 -d [hosts] [retries 1-99]. This is the centralized, automatic launcher.",
      "starts the real dsupdt process", GREEN],
    ["3","PBS batch job",
      "The dsupdt process currently runs as a PBS batch job \u2014 -d PBS with -QS qsub options; -PL forks parallel records for heavy datasets.",
      "runs download \u2192 build \u2192 archive", AMBER],
  ];
  const y=2.1, gap=0.55, cw=(12.33-2*gap)/3, ch=3.5; let x=0.5;
  cards.forEach((c,i)=>{
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color:LIGHT}, line:{color:c[4], width:2},
      shadow:{type:"outer", color:"9FB6C6", blur:6, offset:2, angle:90, opacity:0.3} });
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:0.8, rectRadius:0.1,
      fill:{color:c[4]}, line:{type:"none"} });
    s.addShape(p.ShapeType.rect, { x, y:y+0.42, w:cw, h:0.38, fill:{color:c[4]}, line:{type:"none"} });
    circ(s, x+0.2, y+0.18, 0.44, LIGHT, c[0], c[4], 15);
    s.addText(c[1], { x:x+0.72, y:y+0.05, w:cw-0.9, h:0.7, fontFace:SERIF,
      bold:true, fontSize:15.5, color:LIGHT, margin:0, valign:"middle" });
    s.addText(c[2], { x:x+0.25, y:y+0.95, w:cw-0.5, h:1.75, fontFace:SANS,
      fontSize:12.5, color:INK, margin:0, valign:"top", lineSpacingMultiple:1.12 });
    s.addShape(p.ShapeType.roundRect, { x:x+0.22, y:y+2.78, w:cw-0.44, h:0.55,
      rectRadius:0.05, fill:{color:TINT2}, line:{type:"none"} });
    s.addText([
      { text:"\u2192  ", options:{ bold:true, color:c[4] } },
      { text:c[3], options:{ bold:true, color:c[4] } },
    ], { x:x+0.36, y:y+2.78, w:cw-0.6, h:0.55, fontFace:SANS,
      fontSize:12, margin:0, valign:"middle" });
    if (i<2) s.addText("\u2192", { x:x+cw+0.02, y:y+1.2, w:gap-0.04, h:0.8,
      align:"center", valign:"middle", fontFace:SANS, fontSize:26, bold:true,
      color:MUTE, margin:0 });
    x += cw+gap;
  });
  // manual / direct note strip
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.9, w:12.33, h:0.9, rectRadius:0.1,
    fill:{color:TINT}, line:{color:LINE, width:1} });
  circ(s, 0.75, 6.15, 0.5, DEEP, "\u25B8", LIGHT, 16);
  s.addText([
    { text:"Direct / manual: ", options:{ bold:true, color:DEEP } },
    { text:"run ", options:{} },
    { text:"dsupdt -UF", options:{ fontFace:MONO, bold:true } },
    { text:" yourself \u2014 or from a plain cron job on a specific host \u2014 for one-off runs, re-archiving, and debugging. No control record or dscheck needed.", options:{} },
  ], { x:1.4, y:5.9, w:11.2, h:0.9, fontFace:SANS, fontSize:12.5,
       color:INK, valign:"middle", margin:0, lineSpacingMultiple:1.05 });
  foot(s);
})();

// ============================================================ 15 VALID INTERVAL / MULTI-PERIOD
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Concepts", TEAL); title(s, "Valid Interval & Multi-Period Updates");
  // left concepts
  const items = [
    ["-FQ  Frequency","the size of one update period (1D, 6H, 1M, \u2026)", DEEP],
    ["-MU  MultipleUpdate","process every elapsed period, not just the newest", GREEN],
    ["-VI  ValidInterval","look back this far to re-check / re-archive late data", AMBER],
    ["-DI  DueInterval","lead time from period end until data is expected", TEAL],
  ];
  let iy=1.75;
  items.forEach(it=>{
    s.addShape(p.ShapeType.roundRect, { x:0.5, y:iy, w:6.1, h:1.05, rectRadius:0.08,
      fill:{color:TINT}, line:{color:it[2], width:1.5} });
    s.addText(it[0], { x:0.72, y:iy+0.12, w:5.7, h:0.4, fontFace:MONO, bold:true,
      fontSize:15, color:it[2], margin:0 });
    s.addText(it[1], { x:0.72, y:iy+0.55, w:5.7, h:0.42, fontFace:SANS,
      fontSize:12.5, color:INK, margin:0, valign:"top" });
    iy += 1.18;
  });
  // right: window illustration
  s.addShape(p.ShapeType.roundRect, { x:6.9, y:1.75, w:5.93, h:3.05, rectRadius:0.1,
    fill:{color:MID}, line:{type:"none"} });
  s.addText("Re-check window  (VI = 4D, FQ = 1D)", { x:7.15, y:1.9, w:5.5, h:0.4,
    fontFace:SANS, bold:true, fontSize:14, color:AMBER, margin:0 });
  // timeline
  const tlx=7.25, tlw=5.2, tly=2.9;
  s.addShape(p.ShapeType.line, { x:tlx, y:tly, w:tlw, h:0, line:{color:"5A7A94", width:2} });
  for (let k=0;k<5;k++){
    const px = tlx + k*(tlw/4);
    const past = k<4;
    s.addShape(p.ShapeType.ellipse, { x:px-0.11, y:tly-0.11, w:0.22, h:0.22,
      fill:{color: past?AMBER:"9FE0C9"}, line:{type:"none"} });
  }
  s.addText("now \u2212 VI", { x:tlx-0.4, y:tly+0.2, w:1.2, h:0.3, fontFace:SANS,
    fontSize:10, color:"9FC4E0", margin:0, align:"center" });
  s.addText("now", { x:tlx+tlw-0.6, y:tly+0.2, w:1.2, h:0.3, fontFace:SANS,
    fontSize:10, color:"9FC4E0", margin:0, align:"center" });
  s.addText([
    { text:"Each period is re-checked. Already-archived periods report \u201Cno newer file\u201D; a newer source file triggers ", options:{ color:"D8E6F0" } },
    { text:"re-archive", options:{ color:"9FE0C9", bold:true } },
    { text:".  VI = 0 \u2192 single period, no newer-file check.", options:{ color:"D8E6F0" } },
  ], { x:7.15, y:3.5, w:5.5, h:1.2, fontFace:SANS, fontSize:12.5, margin:0,
       valign:"top", lineSpacingMultiple:1.12 });

  s.addShape(p.ShapeType.roundRect, { x:6.9, y:4.95, w:5.93, h:1.65, rectRadius:0.1,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText("Partial updates", { x:7.15, y:5.08, w:5.5, h:0.35, fontFace:SANS,
    bold:true, fontSize:14, color:DEEP, margin:0 });
  s.addText([
    { text:"With -MR Y", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:", a many-to-one build tolerates missing remotes. If -VI is also set, the update is held open until the files arrive or the window expires.", options:{ color:INK } },
  ], { x:7.15, y:5.45, w:5.5, h:1.1, fontFace:SANS, fontSize:12.5, margin:0,
       valign:"top", lineSpacingMultiple:1.12 });

  s.addText([
    { text:"-IE / -QE / -EC", options:{ fontFace:MONO, bold:true, color:AMBER } },
    { text:"  govern how errors interact with multi-period runs: skip & continue, quit, or normal.", options:{} },
  ], { x:0.5, y:6.55, w:6.3, h:0.5, fontFace:SANS, fontSize:12, color:MUTE,
       margin:0, valign:"top", lineSpacingMultiple:1.05 });
  foot(s);
})();

// ============================================================ 15B TEMPORAL TIMELINE
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Concepts", TEAL); title(s, "How the Temporal Intervals Fit Together");
  s.addText([
    { text:"Data advances one ", options:{} },
    { text:"Frequency (FQ)", options:{ bold:true, color:DEEP } },
    { text:" per period. A single ", options:{} },
    { text:"-MU", options:{ fontFace:MONO, bold:true, color:GREEN } },
    { text:" run walks every elapsed period inside the ", options:{} },
    { text:"Valid Interval (VI)", options:{ bold:true, color:"B5701F" } },
    { text:" look-back window, while the ", options:{} },
    { text:"Due Interval (DI)", options:{ bold:true, color:TEAL } },
    { text:" sets when each period becomes due.", options:{} },
  ], { x:0.5, y:1.5, w:12.3, h:0.55, fontFace:SANS, fontSize:14, color:INK,
       margin:0, valign:"top", lineSpacingMultiple:1.1 });

  const tlx=1.3, tlw=10.2, dx=tlw/6, ty=3.55;
  const xAt = k => tlx + k*dx;
  const periods = [
    {d:"07-09", c:GREEN, lbl:""},
    {d:"07-10", c:GREEN, lbl:""},
    {d:"07-11", c:TEAL,  lbl:"End Date"},
    {d:"07-12", c:AMBER, lbl:""},
    {d:"07-13", c:AMBER, lbl:""},
    {d:"07-14", c:null,  lbl:""},
    {d:"07-15", c:null,  lbl:"now"},
  ];

  // VI look-back band (now-VI .. now) => periods k=2..6
  const bx = xAt(2)-0.18, bw = (xAt(6)-xAt(2))+0.36;
  s.addShape(p.ShapeType.roundRect, { x:bx, y:2.55, w:bw, h:1.55, rectRadius:0.08,
    fill:{color:"FBEDDC"}, line:{color:AMBER, width:1.25, dashType:"dash"} });
  s.addText("VI \u2014 look-back re-check window   (now \u2212 VI \u2192 now)", {
    x:bx+0.15, y:2.62, w:bw-0.3, h:0.32, fontFace:SANS, bold:true, fontSize:12,
    color:"B5701F", margin:0 });

  // axis
  s.addShape(p.ShapeType.line, { x:tlx-0.2, y:ty, w:tlw+0.4, h:0, line:{color:"8AA6BC", width:2} });

  // FQ bracket over the first period gap (above axis)
  const fq0=xAt(0), fq1=xAt(1);
  s.addShape(p.ShapeType.line, { x:fq0, y:ty-0.42, w:fq1-fq0, h:0, line:{color:DEEP, width:1.5} });
  s.addShape(p.ShapeType.line, { x:fq0, y:ty-0.42, w:0, h:0.22, line:{color:DEEP, width:1.5} });
  s.addShape(p.ShapeType.line, { x:fq1, y:ty-0.42, w:0, h:0.22, line:{color:DEEP, width:1.5} });
  s.addText("FQ \u2014 one period", { x:fq0-0.3, y:ty-0.78, w:(fq1-fq0)+0.6, h:0.3,
    align:"center", fontFace:SANS, bold:true, fontSize:11, color:DEEP, margin:0 });

  // ticks, date labels, End Date / now markers
  periods.forEach((pd,k)=>{
    const px = xAt(k);
    if (pd.c) {
      s.addShape(p.ShapeType.ellipse, { x:px-0.13, y:ty-0.13, w:0.26, h:0.26, fill:{color:pd.c}, line:{type:"none"} });
    } else {
      s.addShape(p.ShapeType.ellipse, { x:px-0.13, y:ty-0.13, w:0.26, h:0.26, fill:{color:LIGHT}, line:{color:MUTE, width:2} });
    }
    s.addText(pd.d, { x:px-0.6, y:ty+0.22, w:1.2, h:0.3, align:"center", fontFace:MONO, fontSize:11, color:MUTE, margin:0 });
    if (pd.lbl==="End Date") {
      s.addShape(p.ShapeType.line, { x:px, y:2.5, w:0, h:1.65, line:{color:TEAL, width:1.5, dashType:"dash"} });
      s.addText("End Date", { x:px-0.9, y:2.18, w:1.8, h:0.3, align:"center", fontFace:SANS, bold:true, fontSize:11, color:TEAL, margin:0 });
    }
    if (pd.lbl==="now") {
      s.addShape(p.ShapeType.line, { x:px, y:2.5, w:0, h:1.65, line:{color:INK, width:1.5, dashType:"dash"} });
      s.addText("now", { x:px-0.6, y:2.18, w:1.2, h:0.3, align:"center", fontFace:SANS, bold:true, fontSize:11, color:INK, margin:0 });
    }
  });

  // next-due cutoff = now - DI: periods to its right are not yet due (end + DI > now)
  const cutx = xAt(6) - dx*1.4;
  s.addShape(p.ShapeType.line, { x:cutx, y:2.5, w:0, h:1.65, line:{color:MUTE, width:1.25, dashType:"dash"} });
  s.addText("due cutoff\n(now \u2212 DI)", { x:cutx-1.25, y:2.02, w:2.5, h:0.46, align:"center",
    fontFace:SANS, bold:true, fontSize:10.5, color:MUTE, margin:0, lineSpacingMultiple:0.95 });

  // DI arrow (below axis) from End Date to next-due
  const di0=xAt(2), di1=xAt(2)+dx*1.4;
  s.addShape(p.ShapeType.line, { x:di0, y:ty+0.62, w:di1-di0, h:0, line:{color:TEAL, width:1.75, endArrowType:"triangle"} });
  s.addText("DI \u2014 next-due = period end + DI", { x:di0-0.15, y:ty+0.72, w:3.9, h:0.3,
    fontFace:SANS, bold:true, fontSize:11, color:TEAL, margin:0 });

  // ControlTime marker (below axis, under 'now'): the scheduled wall-clock run.
  // Each success steps CT by ONE FQ (Next CT = CT + FQ); CO only offsets the run
  // from the period start, so it cancels in the step. Square marker to stand apart from round data ticks.
  const ctx=xAt(6), ctY=4.42;
  s.addShape(p.ShapeType.rect, { x:ctx-0.10, y:ctY-0.10, w:0.20, h:0.20, fill:{color:DEEP}, line:{type:"none"} });
  s.addText("CT \u2014 job runs", { x:ctx-2.25, y:ctY-0.13, w:2.0, h:0.26, align:"right",
    fontFace:SANS, bold:true, fontSize:11, color:DEEP, margin:0, valign:"middle" });
  s.addText("= period start + CO", { x:ctx-2.25, y:ctY+0.11, w:2.0, h:0.22, align:"right",
    fontFace:SANS, italic:true, fontSize:9, color:MUTE, margin:0 });
  const cnx=13.0;
  s.addShape(p.ShapeType.line, { x:ctx+0.14, y:ctY, w:cnx-ctx-0.26, h:0, line:{color:DEEP, width:1.5, dashType:"dash", endArrowType:"triangle"} });
  s.addShape(p.ShapeType.rect, { x:cnx-0.09, y:ctY-0.09, w:0.18, h:0.18, fill:{color:LIGHT}, line:{color:DEEP, width:2} });
  s.addText("+ FQ", { x:ctx+0.10, y:ctY-0.32, w:1.6, h:0.24, align:"center",
    fontFace:SANS, fontSize:10, color:DEEP, margin:0 });
  s.addText("next CT", { x:cnx-0.75, y:ctY+0.10, w:1.5, h:0.24, align:"center",
    fontFace:SANS, italic:true, fontSize:9.5, color:MUTE, margin:0 });

  // tick-state legend
  const leg = [
    [GREEN, "Archived earlier"],
    [TEAL,  "Re-checked \u2014 no newer source"],
    [AMBER, "Archived / re-archived this run"],
    [null,  "Not yet due (waiting DI)"],
  ];
  let lx=0.7; const lyy=4.85;
  leg.forEach(g=>{
    if (g[0]) s.addShape(p.ShapeType.ellipse, { x:lx, y:lyy, w:0.22, h:0.22, fill:{color:g[0]}, line:{type:"none"} });
    else s.addShape(p.ShapeType.ellipse, { x:lx, y:lyy, w:0.22, h:0.22, fill:{color:LIGHT}, line:{color:MUTE, width:2} });
    s.addText(g[1], { x:lx+0.3, y:lyy-0.06, w:2.9, h:0.34, fontFace:SANS, fontSize:11.5, color:INK, margin:0, valign:"middle" });
    lx += 3.05;
  });

  // ControlTime -- the scheduled wall-clock run time (separate from the data periods above)
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:5.3, w:12.33, h:0.82, rectRadius:0.08,
    fill:{color:TINT}, line:{color:DEEP, width:1} });
  s.addText("ControlTime  \u2014  when the job itself runs", { x:0.72, y:5.37, w:11.8, h:0.3,
    fontFace:SANS, bold:true, fontSize:12.5, color:DEEP, margin:0 });
  s.addText([
    { text:"-CT", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:" sets the run time (YYYY-MM-DD HH:NN:SS). Each success steps it by one ", options:{} },
    { text:"-FQ", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:" (Next CT = CT + FQ); ", options:{} },
    { text:"-CO", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:" only offsets the run from the period start (daily \u2192 day start + CO), so it cancels in the step. Failure retries after ", options:{} },
    { text:"-RI", options:{ fontFace:MONO, bold:true, color:DEEP } },
    { text:".   e.g.  ", options:{} },
    { text:"dsupdt d609000 -SU -CT \"2026-07-11 09:00:00\" -FQ 1D -CO 9H", options:{ fontFace:MONO, color:TEAL } },
  ], { x:0.72, y:5.68, w:11.9, h:0.4, fontFace:SANS, fontSize:11.5, color:INK, margin:0, valign:"middle" });

  // summary strip
  s.addShape(p.ShapeType.roundRect, { x:0.5, y:6.24, w:12.33, h:0.6, rectRadius:0.08,
    fill:{color:TINT2}, line:{color:LINE, width:1} });
  s.addText([
    { text:"Each run re-checks every period in the VI window: ", options:{ bold:true, color:DEEP } },
    { text:"already-archived periods report \u201Cno newer file\u201D, newly available periods are archived, and periods still within DI of now are left for the next run.  ", options:{} },
    { text:"VI = 0 \u2192 only the most recent period.", options:{ italic:true, color:MUTE } },
  ], { x:0.75, y:6.24, w:11.85, h:0.6, fontFace:SANS, fontSize:11.5, color:INK,
       margin:0, valign:"middle", lineSpacingMultiple:1.05 });

  foot(s);
})();

// ============================================================ 16 EMAIL REPORTING
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Reporting", AMBER); title(s, "Email Reporting  (-MC / -EMailControl)");
  const codes = [
    ["A","All","full detailed report, always", GREEN],
    ["S","Summary","summary only, no detail", TEAL],
    ["E","Error","detailed report, only on error", AMBER],
    ["B","Summary on error","summary, only on error", DEEP],
    ["N","None","no email at all", MUTE],
  ];
  const y0=1.7, cw=2.42, ch=1.85, gap=0.15; let x=0.5;
  codes.forEach(c=>{
    s.addShape(p.ShapeType.roundRect, { x, y:y0, w:cw, h:ch, rectRadius:0.1,
      fill:{color:LIGHT}, line:{color:c[3], width:2},
      shadow:{type:"outer", color:"9FB6C6", blur:4, offset:2, angle:90, opacity:0.25} });
    circ(s, x+cw/2-0.4, y0+0.22, 0.8, c[3], c[0], LIGHT, 30);
    s.addText(c[1], { x:x+0.1, y:y0+1.05, w:cw-0.2, h:0.4, align:"center",
      fontFace:SANS, bold:true, fontSize:14, color:INK, margin:0 });
    s.addText(c[2], { x:x+0.12, y:y0+1.42, w:cw-0.24, h:0.4, align:"center",
      fontFace:SANS, fontSize:11, color:MUTE, margin:0, valign:"top",
      lineSpacingMultiple:1.0 });
    x += cw+gap;
  });
  // report sections
  s.addText("Report sections", { x:0.5, y:3.9, w:6, h:0.4, fontFace:SANS,
    bold:true, fontSize:16, color:DEEP, margin:0 });
  const secs = [
    ["Error","real errors \u2014 survives in every mode; raw failures are prefixed with the failing command / dataset", "C0392B"],
    ["Summary","headline count + rolled-up \u201CN files ARCHIVED\u201D range lines", DEEP],
    ["Detail","per-file lines; consecutive no-op re-checks collapse into one range line", TEAL],
  ];
  let sy=4.4;
  secs.forEach(sec=>{
    s.addShape(p.ShapeType.roundRect, { x:0.5, y:sy, w:12.33, h:0.68, rectRadius:0.06,
      fill:{color:TINT}, line:{color:LINE, width:1} });
    s.addShape(p.ShapeType.roundRect, { x:0.5, y:sy, w:1.9, h:0.68, rectRadius:0.06,
      fill:{color:sec[2]}, line:{type:"none"} });
    s.addText(sec[0], { x:0.5, y:sy, w:1.9, h:0.68, align:"center", valign:"middle",
      fontFace:SANS, bold:true, fontSize:14, color:LIGHT, margin:0 });
    s.addText(sec[1], { x:2.6, y:sy, w:10.05, h:0.68, fontFace:SANS, fontSize:13,
      color:INK, margin:0, valign:"middle" });
    sy += 0.78;
  });
  s.addText([
    { text:"Related:  ", options:{ bold:true, color:AMBER } },
    { text:"-EE", options:{ fontFace:MONO, bold:true } },
    { text:" (=E),  ", options:{} },
    { text:"-SE", options:{ fontFace:MONO, bold:true } },
    { text:" (=S),  ", options:{} },
    { text:"-NE", options:{ fontFace:MONO, bold:true } },
    { text:" (=N),  ", options:{} },
    { text:"-CC", options:{ fontFace:MONO, bold:true } },
    { text:" adds carbon-copy recipients.", options:{} },
  ], { x:0.5, y:6.85, w:12.3, h:0.3, fontFace:SANS, fontSize:11.5, color:MUTE,
       align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 17 KEY MODE OPTIONS
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Reference", TEAL); title(s, "Key Mode Options at a Glance");
  const opts = [
    ["-MU","process all elapsed periods"],
    ["-FU","force one update even if not due"],
    ["-CP","allow current, not-yet-due period"],
    ["-MO","only not-yet-archived periods"],
    ["-RA","force re-archive via dsarch"],
    ["-RD","re-download existing remote file"],
    ["-CN","re-download if changed on server"],
    ["-RE","reset end date/hour from file time"],
    ["-IE","skip errors, continue (with -MU)"],
    ["-QE","quit dataset on first error"],
    ["-GZ","use GMT as controlling time"],
    ["-NY","skip Feb 29 in leap years"],
    ["-KR","keep remote file (copy, not move)"],
    ["-KS","keep server file (copy, not move)"],
    ["-UB","use period begin time for patterns"],
    ["-UT","force end/next-due time advance"],
    ["-BG","background; suppress screen output"],
    ["-NE","suppress notification email"],
  ];
  const y0=1.75, cw=4.05, ch=0.62, gapx=0.09, gapy=0.12;
  opts.forEach((o,i)=>{
    const col=i%3, row=Math.floor(i/3);
    const x=0.5+col*(cw+gapx), y=y0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.06,
      fill:{color: row%2?LIGHT:TINT}, line:{color:LINE, width:1} });
    s.addShape(p.ShapeType.roundRect, { x:x+0.1, y:y+0.13, w:0.85, h:0.36,
      rectRadius:0.05, fill:{color:DEEP}, line:{type:"none"} });
    s.addText(o[0], { x:x+0.1, y:y+0.13, w:0.85, h:0.36, align:"center",
      valign:"middle", fontFace:MONO, bold:true, fontSize:12, color:LIGHT, margin:0 });
    s.addText(o[1], { x:x+1.05, y:y, w:cw-1.15, h:ch, fontFace:SANS, fontSize:11.5,
      color:INK, margin:0, valign:"middle", lineSpacingMultiple:0.95 });
  });
  s.addText("Most modes can be preset in the control record via -UC (single letters) and -KF / -MC / -EC.", {
    x:0.5, y:6.85, w:12.3, h:0.3, fontFace:SANS, italic:true, fontSize:12,
    color:MUTE, align:"center", margin:0 });
  foot(s);
})();

// ============================================================ 18 ENVIRONMENT
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Developer configuration", DEEP); title(s, "Environment & Prerequisites");
  const items = [
    ["\u2318","Under dsarch/dsupdt","All operational datasets run under dsupdt/dsarch control; dsupdt hands its local files to dsarch for archiving onto GDEX.", DEEP],
    ["$","$UPDTWKP","Root for working dirs. If set to /lustre/gdex/work, then $UPDTWKP/zji/icoads resolves under it; if unset, -WD is used as given.", TEAL],
    ["\uD83D\uDD12","Ownership","Only the owning specialist may run a record. Use -MD to override, or -LN to run on behalf of another (auto-adds -MD to dsarch).", AMBER],
    ["d","dNNNNNN.*","Input files must start with the dataset number and match -DS \u2014 a guard against operating on the wrong dataset.", GREEN],
    ["\u2225","-PL parallelism","Fork N child processes, each handling one record, for datasets with many independent, time-consuming updates.", DEEP],
    ["\u2709","-DB / logs","Debug logging via -DB level [path] [file]; default log path $DSSHOME/dssdb/log, file pgdss.dbg.", TEAL],
  ];
  const y0=1.7, cw=6.05, ch=1.5, gapx=0.23, gapy=0.2; let i=0;
  items.forEach(it=>{
    const col=i%2, row=Math.floor(i/2);
    const x=0.5+col*(cw+gapx), y=y0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.08,
      fill:{color:LIGHT}, line:{color:LINE, width:1},
      shadow:{type:"outer", color:"9FB6C6", blur:4, offset:2, angle:90, opacity:0.25} });
    circ(s, x+0.24, y+0.24, 0.62, it[3], it[0], LIGHT, 18);
    s.addText(it[1], { x:x+1.0, y:y+0.2, w:cw-1.2, h:0.4, fontFace:SANS,
      bold:true, fontSize:15, color:INK, margin:0, valign:"middle" });
    s.addText(it[2], { x:x+1.0, y:y+0.62, w:cw-1.2, h:0.8, fontFace:SANS,
      fontSize:11.5, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.05 });
    i++;
  });
  foot(s);
})();

// ============================================================ 19 TROUBLESHOOTING
(() => {
  const s = p.addSlide(); s.background = { color: LIGHT };
  kicker(s, "Operations", AMBER); title(s, "Troubleshooting & Recovery");
  const cards = [
    ["Stuck lock","A crashed run may leave PID/host on a record, blocking reprocessing on another host.",
      "dsupdt -UL -CI 57\ndsupdt -UL -LI 33", DEEP],
    ["Is data ready?","Test remote-file availability before committing to an update.",
      "dsupdt d540000 -CU -CA", TEAL],
    ["Re-run a period","Substitute a different date to re-archive or recover a deleted action.",
      "dsupdt d540000 -UF -CD 2026-07-01 -RA", AMBER],
    ["Retry on failure","Control records retry automatically after -RI; -EC sets error behavior.",
      "dsupdt d540000 -SC -CI 5 -RI 12H -EC N", GREEN],
  ];
  const y0=1.7, cw=6.05, ch=2.15, gapx=0.23, gapy=0.25; let i=0;
  cards.forEach(c=>{
    const col=i%2, row=Math.floor(i/2);
    const x=0.5+col*(cw+gapx), y=y0+row*(ch+gapy);
    s.addShape(p.ShapeType.roundRect, { x, y, w:cw, h:ch, rectRadius:0.1,
      fill:{color:LIGHT}, line:{color:c[3], width:2},
      shadow:{type:"outer", color:"9FB6C6", blur:5, offset:2, angle:90, opacity:0.28} });
    circ(s, x+0.25, y+0.25, 0.55, c[3], String.fromCharCode(9679), LIGHT, 10);
    s.addText(c[0], { x:x+0.95, y:y+0.22, w:cw-1.15, h:0.45, fontFace:SERIF,
      bold:true, fontSize:18, color:INK, margin:0, valign:"middle" });
    s.addText(c[1], { x:x+0.3, y:y+0.85, w:cw-0.6, h:0.7, fontFace:SANS,
      fontSize:12.5, color:MUTE, margin:0, valign:"top", lineSpacingMultiple:1.08 });
    s.addShape(p.ShapeType.roundRect, { x:x+0.28, y:y+ch-0.85, w:cw-0.56, h:0.68,
      rectRadius:0.05, fill:{color:MID}, line:{type:"none"} });
    s.addText(c[2], { x:x+0.42, y:y+ch-0.85, w:cw-0.8, h:0.68, fontFace:MONO,
      fontSize:11, color:"9FE0C9", margin:0, valign:"middle", lineSpacingMultiple:0.95 });
    i++;
  });
  foot(s);
})();

// ============================================================ 20 CLOSING
(() => {
  const s = p.addSlide(); s.background = { color: MID };
  [3.0,2.2,1.4].forEach((d,i)=> s.addShape(p.ShapeType.ellipse,
    { x:11.6-d/2, y:6.2-d/2, w:d, h:d, fill:{type:"none"},
      line:{color:i===2?AMBER:"1C3A55", width:i===2?2:1} }));
  s.addText("Recap", { x:0.7, y:0.75, w:8, h:0.4, fontFace:SANS, bold:true,
    fontSize:13, color:AMBER, charSpacing:2, margin:0 });
  s.addText("From Config to Archive", { x:0.7, y:1.15, w:11, h:0.8, fontFace:SERIF,
    bold:true, fontSize:38, color:LIGHT, margin:0 });
  const points = [
    ["Configure","three record types: control (dcupdt), local (dlupdt), remote (drupdt)"],
    ["Run","-UF drives download \u2192 build \u2192 archive \u2192 clean in one command"],
    ["Automate","link to a control record; dscheck dispatches, retries, and reports"],
    ["Tune","patterns, valid interval, parallelism, and email controls per record"],
  ];
  let yy=2.3;
  points.forEach((pt,i)=>{
    circ(s, 0.7, yy, 0.55, i%2?TEAL:DEEP, String(i+1), LIGHT, 18);
    s.addText([
      { text:pt[0]+"  ", options:{ bold:true, color:LIGHT } },
      { text:pt[1], options:{ color:"9FC4E0" } },
    ], { x:1.45, y:yy, w:9.5, h:0.55, fontFace:SANS, fontSize:15, margin:0, valign:"middle" });
    yy += 0.78;
  });
  s.addShape(p.ShapeType.roundRect, { x:0.7, y:5.75, w:11.9, h:1.1, rectRadius:0.1,
    fill:{color:"16273D"}, line:{color:"2A425C", width:1} });
  s.addText([
    { text:"Learn more   ", options:{ bold:true, color:AMBER } },
    { text:"dsupdt -h <OPT>", options:{ fontFace:MONO, color:"9FE0C9" } },
    { text:"   \u2022   full guide: dsupdt.usg   \u2022   docs: gdex-docs-dsupdt.readthedocs.io   \u2022   repo: github.com/NCAR/rda-python-dsupdt", options:{ color:"8FB0C9" } },
  ], { x:0.95, y:5.75, w:11.4, h:1.1, fontFace:SANS, fontSize:13, margin:0,
       valign:"middle", lineSpacingMultiple:1.1 });
})();

// ============================================================ 21 QUESTIONS
(() => {
  const s = p.addSlide(); s.background = { color: MID };
  // concentric motif, centered behind the mark
  [4.4,3.3,2.2].forEach((d,i)=> s.addShape(p.ShapeType.ellipse,
    { x:W/2-d/2, y:2.55-d/2, w:d, h:d, fill:{type:"none"},
      line:{color:i===2?AMBER:"1C3A55", width:i===2?2:1} }));
  circ(s, W/2-0.55, 2.0, 1.1, AMBER, "?", MID, 46);
  s.addText("Any Questions?", { x:0.5, y:3.55, w:12.33, h:0.9, fontFace:SERIF,
    bold:true, fontSize:52, color:LIGHT, align:"center", margin:0 });
  s.addText("Thank you", { x:0.5, y:4.55, w:12.33, h:0.5, fontFace:SANS, italic:true,
    fontSize:18, color:"8FB0C9", align:"center", margin:0 });
  s.addShape(p.ShapeType.roundRect, { x:W/2-4.75, y:5.55, w:9.5, h:0.9, rectRadius:0.1,
    fill:{color:"16273D"}, line:{color:"2A425C", width:1} });
  s.addText([
    { text:"dsupdt -h <OPT>", options:{ fontFace:MONO, color:"9FE0C9" } },
    { text:"   \u2022   dsupdt.usg   \u2022   ", options:{ color:"8FB0C9" } },
    { text:"github.com/NCAR/rda-python-dsupdt", options:{ color:"8FB0C9" } },
  ], { x:W/2-4.75, y:5.55, w:9.5, h:0.9, fontFace:SANS, fontSize:13, align:"center",
       valign:"middle", margin:0 });
})();

p.writeFile({ fileName: "dsupdt_guide.pptx" }).then(f => console.log("wrote", f));
