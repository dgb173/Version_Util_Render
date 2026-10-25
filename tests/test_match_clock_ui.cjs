const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const text = fs.readFileSync('src/templates/precacheo.html', 'utf8');
const script = text.slice(text.indexOf('        function getSpainParts('), text.indexOf('        function updateCurrentTimeDisplay('));
for (const tz of ['UTC', 'Europe/Madrid', 'America/New_York']) {
    process.env.TZ = tz;
    const ctx = {Intl, Date, precacheDateFormatHint: 'MDY', parseMatchTime: s => {
        const m = String(s).match(/(\d+):(\d+)/);
        return m ? {hours: Number(m[1]), minutes: Number(m[2])} : null;
    }};
    vm.createContext(ctx);vm.runInContext(script, ctx);
    assert.equal(ctx.getSpainTomorrowDateKey(new Date('2026-10-24T22:30:00Z')), '2026-10-26');
    assert.equal(ctx.getSpainTomorrowDateKey(new Date('2026-03-28T22:30:00Z')), '2026-03-29');
    assert.equal(ctx.getSpainTomorrowDateKey(new Date('2026-12-31T22:30:00Z')), '2027-01-01');
    const aware = ctx.getMatchScheduleParts({start_time: '2026-10-25T20:00:00Z'});
    assert.equal(aware.hour, 21);
    const naive = ctx.getMatchScheduleParts({start_time: '2026-10-25T20:00:00', match_date: '10/25/2026', time: '20:00'});
    assert.equal(naive.hour, 20);
}
console.log('Madrid calendar and display: DST and three host timezones passed');
