"""Offline affine angle fit. Measured fit points and held-out checks stay separate."""
import argparse,json,math
from pathlib import Path

def fit_angle_points(points):
    fit=[p for p in points if p['role']=='fit']
    checks=[p for p in points if p['role']=='validation']
    if len(fit)<2:raise ValueError('At least two measured fit points required')
    for p in points:
        if not p.get('measurement_confirmed'):raise ValueError('Unconfirmed angle measurement')
        for key in ['native_encoder','measured_angle_deg']:
            if type(p[key]) not in (int,float) or not math.isfinite(p[key]):raise ValueError('Nonfinite/non-numeric measurement')
    xs=[p['native_encoder'] for p in fit];ys=[p['measured_angle_deg'] for p in fit]
    if max(xs)-min(xs)>2048:raise ValueError('Possible encoder wrap: establish unwrapped coordinates first')
    xbar=sum(xs)/len(xs);ybar=sum(ys)/len(ys);var=sum((x-xbar)**2 for x in xs)
    if var==0:raise ValueError('Distinct encoder positions required')
    slope=sum((x-xbar)*(y-ybar) for x,y in zip(xs,ys))/var
    if slope==0:raise ValueError('Measured angular excursion required')
    offset=ybar-slope*xbar
    evaluated=[{**p,'predicted_angle_deg':slope*p['native_encoder']+offset,
                'error_deg':slope*p['native_encoder']+offset-p['measured_angle_deg']} for p in points]
    return {'slope_deg_per_native_count':slope,'offset_deg':offset,'fit_points':len(fit),
            'independent_validation_points':len(checks),'points':evaluated,
            'precision_accepted':False,'status':'candidate_pending_independent_validation_and_uncertainty',
            'note':'Two-point fit residuals cannot establish accuracy; model zero binding is separate.'}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('input',type=Path);p.add_argument('output',type=Path);a=p.parse_args()
    r=fit_angle_points(json.loads(a.input.read_text(encoding='utf-8'))['points'])
    with a.output.open('x',encoding='utf-8') as f:json.dump(r,f,ensure_ascii=False,indent=2)
    print(json.dumps(r,ensure_ascii=False))
