
/*******************************************************************************
r.sun: rsunlib.c. This program was writen by Jaro Hofierka in Summer 1993 and re-engineered
in 1996-1999. In cooperation with Marcel Suri and Thomas Huld from JRC in Ispra
a new version of r.sun was prepared using ESRA solar radiation formulas.
See manual pages for details.
(C) 2002 Copyright Jaro Hofierka, Gresaka 22, 085 01 Bardejov, Slovakia, 
              and GeoModel, s.r.o., Bratislava, Slovakia
email: hofierka@geomodel.sk,marcel.suri@jrc.it,suri@geomodel.sk
*******************************************************************************/
/*
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the
 *   Free Software Foundation, Inc.,
 *   59 Temple Place - Suite 330,
 *   Boston, MA  02111-1307, USA.
 */

/*v. 2.0 July 2002, NULL data handling, JH */
/*v. 2.1 January 2003, code optimization by Thomas Huld, JH */

#include <stdio.h>
#include <math.h>
#include <grass/gis.h>
#include <grass/gprojects.h>
#include <grass/glocale.h>
#include "sunradstruct.h"
#include "local_proto.h"
#include "rsunglobals.h"


int civilTimeFlag;
int useCivilTime()
{
    return civilTimeFlag;
}
void setUseCivilTime(int val)
{
    civilTimeFlag = val;
}


double angular_loss_denom;

void setAngularLossDenominator()
{
    angular_loss_denom = 1. / (1 - exp(-1. / a_r));

}


int useShadowFlag;
int useShadow()
{
    return useShadowFlag;
}
void setUseShadow(int val)
{
    useShadowFlag = val;
}



int useHorizonDataFlag;
int useHorizonData()
{
    return useHorizonDataFlag;
}
void setUseHorizonData(int val)
{
    useHorizonDataFlag = val;
}

double timeOffset;
double getTimeOffset()
{
    return timeOffset;
}
void setTimeOffset(double val)
{
    timeOffset = val;
}

double horizonInterval;
double getHorizonInterval()
{
    return horizonInterval;
}
void setHorizonInterval(double val)
{
    horizonInterval = val;
}



double com_sol_const(int no_of_day)
{
    double I0, d1;

    /*  v W/(m*m) */
    d1 = pi2 * no_of_day / 365.25;
    I0 = 1367. * (1 + 0.03344 * cos(d1 - 0.048869));

    return I0;
}




void com_par_const(double longitTime, struct SunGeometryConstDay *sungeom,
		   struct GridGeometry *gridGeom)
{
    double pom;
    double totOffsetTime;

    sungeom->lum_C11 = gridGeom->sinlat * sungeom->cosdecl;
    sungeom->lum_C13 = -gridGeom->coslat * sungeom->sindecl;
    sungeom->lum_C22 = sungeom->cosdecl;
    sungeom->lum_C31 = gridGeom->coslat * sungeom->cosdecl;
    sungeom->lum_C33 = gridGeom->sinlat * sungeom->sindecl;

    if (fabs(sungeom->lum_C31) >= EPS) {
	totOffsetTime = timeOffset + longitTime;

	if (useCivilTime()) {
	    sungeom->timeAngle -= totOffsetTime * HOURANGLE;
	}
	pom = -sungeom->lum_C33 / sungeom->lum_C31;
	if (fabs(pom) <= 1) {
	    pom = acos(pom);
	    pom = (pom * 180) / M_PI;
	    sungeom->sunrise_time = (90 - pom) / 15 + 6;
	    sungeom->sunset_time = (pom - 90) / 15 + 18;
	}
	else {
	    if (pom < 0) {
		/*        G_debug(3,"\n Sun is ABOVE the surface during the whole day"); */
		sungeom->sunrise_time = 0;
		sungeom->sunset_time = 24;
	    }
	    else {
		/*                G_debug(3,"\n The sun is BELOW the surface during the whole day"); */
		if (fabs(pom) - 1 <= EPS) {
		    sungeom->sunrise_time = 12;
		    sungeom->sunset_time = 12;
		}
	    }
	}
    }

}





void com_par(struct SunGeometryConstDay *sungeom,
	     struct SunGeometryVarDay *sunVarGeom,
	     struct GridGeometry *gridGeom, double latitude, double longitude)
{
    double pom, xpom, ypom;

    double costimeAngle;
    double lum_Lx, lum_Ly;

    double newLatitude, newLongitude;
    double inputAngle;
    double delt_lat, delt_lon;
    double delt_east, delt_nor;
    double delt_dist;


    costimeAngle = cos(sungeom->timeAngle);



    lum_Lx = -sungeom->lum_C22 * sin(sungeom->timeAngle);
    lum_Ly = sungeom->lum_C11 * costimeAngle + sungeom->lum_C13;
    sunVarGeom->sinSolarAltitude =
	sungeom->lum_C31 * costimeAngle + sungeom->lum_C33;

    if (fabs(sungeom->lum_C31) < EPS) {
	if (fabs(sunVarGeom->sinSolarAltitude) >= EPS) {
	    if (sunVarGeom->sinSolarAltitude > 0) {
		/*                        G_debug(3,"\tSun is ABOVE area during the whole day"); */
		sungeom->sunrise_time = 0;
		sungeom->sunset_time = 24;
	    }
	    else {
		sunVarGeom->solarAltitude = 0.;
		sunVarGeom->solarAzimuth = UNDEF;
		return;
	    }
	}
	else {
	    /*                      G_debug(3,"\tThe Sun is ON HORIZON during the whole day"); */
	    sungeom->sunrise_time = 0;
	    sungeom->sunset_time = 24;
	}
    }

    sunVarGeom->solarAltitude = asin(sunVarGeom->sinSolarAltitude);	/* vertical angle of the sun */
    /* sinSolarAltitude is sin(solarAltitude) */

    xpom = lum_Lx * lum_Lx;
    ypom = lum_Ly * lum_Ly;
    pom = sqrt(xpom + ypom);


    if (fabs(pom) > EPS) {
	sunVarGeom->solarAzimuth = lum_Ly / pom;
	sunVarGeom->solarAzimuth = acos(sunVarGeom->solarAzimuth);	/* horiz. angle of the Sun */
	/*                      solarAzimuth *= RAD; */
	if (lum_Lx < 0)
	    sunVarGeom->solarAzimuth = pi2 - sunVarGeom->solarAzimuth;
    }
    else {
	sunVarGeom->solarAzimuth = UNDEF;
    }



    if (sunVarGeom->solarAzimuth < 0.5 * M_PI)
	sunVarGeom->sunAzimuthAngle = 0.5 * M_PI - sunVarGeom->solarAzimuth;
    else
	sunVarGeom->sunAzimuthAngle = 2.5 * M_PI - sunVarGeom->solarAzimuth;


    inputAngle = sunVarGeom->sunAzimuthAngle + pihalf;
    inputAngle = (inputAngle >= pi2) ? inputAngle - pi2 : inputAngle;


    delt_lat = -0.0001 * cos(inputAngle);	/* Arbitrary small distance in latitude */
    delt_lon = 0.0001 * sin(inputAngle) / cos(latitude);

    newLatitude = (latitude + delt_lat) * rad2deg;
    newLongitude = (longitude + delt_lon) * rad2deg;


    if ((G_projection() != PROJECTION_LL)) {
	if (pj_do_proj(&newLongitude, &newLatitude, &oproj, &iproj) < 0) {
	    G_fatal_error("Error in pj_do_proj");
	}
    }

    delt_east = newLongitude - gridGeom->xp;
    delt_nor = newLatitude - gridGeom->yp;

    delt_dist = sqrt(delt_east * delt_east + delt_nor * delt_nor);



    sunVarGeom->stepsinangle = gridGeom->stepxy * delt_nor / delt_dist;
    sunVarGeom->stepcosangle = gridGeom->stepxy * delt_east / delt_dist;

    /*
       sunVarGeom->stepsinangle = stepxy * sin(sunVarGeom->sunAzimuthAngle);
       sunVarGeom->stepcosangle = stepxy * cos(sunVarGeom->sunAzimuthAngle);
     */
    sunVarGeom->tanSolarAltitude = tan(sunVarGeom->solarAltitude);

    return;

}


int searching(double *length, struct SunGeometryVarDay *sunVarGeom,
	      struct GridGeometry *gridGeom)
{
    double z2;
    double curvature_diff;
    int succes = 0;

    if (sunVarGeom->zp == UNDEFZ)
	return 0;


    gridGeom->yy0 += sunVarGeom->stepsinangle;
    gridGeom->xx0 += sunVarGeom->stepcosangle;
    if (((gridGeom->xx0 + (0.5 * gridGeom->stepx)) < 0)
	|| ((gridGeom->xx0 + (0.5 * gridGeom->stepx)) > gridGeom->deltx)
	|| ((gridGeom->yy0 + (0.5 * gridGeom->stepy)) < 0)
	|| ((gridGeom->yy0 + (0.5 * gridGeom->stepy)) > gridGeom->delty))
	succes = 3;
    else
	succes = 1;


    if (succes == 1) {
	where_is_point(length, sunVarGeom, gridGeom);
	if (func == NULL) {
	    gridGeom->xx0 = gridGeom->xg0;
	    gridGeom->yy0 = gridGeom->yg0;
	    return (3);
	}
	curvature_diff = EARTHRADIUS * (1. - cos(*length / EARTHRADIUS));

	z2 = sunVarGeom->z_orig + curvature_diff +
	    *length * sunVarGeom->tanSolarAltitude;
	if (z2 < sunVarGeom->zp)
	    succes = 2;		/* shadow */
	if (z2 > sunVarGeom->zmax)
	    succes = 3;		/* no test needed all visible */
    }

    if (succes != 1) {
	gridGeom->xx0 = gridGeom->xg0;
	gridGeom->yy0 = gridGeom->yg0;
    }
    return (succes);
}




double lumcline2(struct SunGeometryConstDay *sungeom,
		 struct SunGeometryVarDay *sunVarGeom,
		 struct SunGeometryVarSlope *sunSlopeGeom,
		 struct GridGeometry *gridGeom, unsigned char *horizonpointer)
{
    double s = 0;
    double length;
    int r = 0;

    int lowPos, highPos;
    double timeoffset, horizPos;
    double horizonHeight;

    func = cube;
    sunVarGeom->isShadow = 0;

    if (useShadow()) {
	length = 0;

	if (useHorizonData()) {
	    /* Start is due east, sungeom->timeangle = -pi/2 */
	    /*
	       timeoffset = sungeom->timeAngle+pihalf;
	     */
	    timeoffset = sunVarGeom->sunAzimuthAngle;

	    /*
	       if(timeoffset<0.)
	       timeoffset+=pi2;
	       else if(timeoffset>pi2)
	       timeoffset-=pi2;
	       horizPos = arrayNumInt - timeoffset/horizonInterval;
	     */

	    horizPos = timeoffset / getHorizonInterval();


	    lowPos = (int)horizPos;
	    highPos = lowPos + 1;
	    if (highPos == arrayNumInt) {
		highPos = 0;
	    }
	    horizonHeight = invScale * ((1. -
					 (horizPos -
					  lowPos)) * horizonpointer[lowPos]
					+ (horizPos - lowPos)
					* horizonpointer[highPos]);
	    sunVarGeom->isShadow =
		(horizonHeight > sunVarGeom->solarAltitude);

	    if (!sunVarGeom->isShadow) {
		/*
		   if (z_orig != UNDEFZ) 
		   {
		   s = sunSlopeGeom->lum_C31_l * cos(-sungeom->timeAngle - sunSlopeGeom->longit_l) + sunSlopeGeom->lum_C33_l; 

		   }
		   else
		   {
		   s = sunVarGeom->sinSolarAltitude;
		   }
		 */
		s = sunSlopeGeom->lum_C31_l * cos(-sungeom->timeAngle - sunSlopeGeom->longit_l) + sunSlopeGeom->lum_C33_l;	/* Jenco */
	    }


	}			/*  End if useHorizonData() */
	else {
	    while ((r = searching(&length, sunVarGeom, gridGeom)) == 1) {
		if (r == 3)
		    break;	/* no test is needed */
	    }




	    if (r == 2) {
		sunVarGeom->isShadow = 1;	/* shadow */
	    }
	    else {

		/*
		   if (z_orig != UNDEFZ) 
		   {

		   s = sunSlopeGeom->lum_C31_l * cos(-sungeom->timeAngle - sunSlopeGeom->longit_l) + sunSlopeGeom->lum_C33_l; 
		   }
		   else
		   {
		   s = sunVarGeom->sinSolarAltitude;
		   }
		 */
		s = sunSlopeGeom->lum_C31_l * cos(-sungeom->timeAngle - sunSlopeGeom->longit_l) + sunSlopeGeom->lum_C33_l;	/* Jenco */
	    }
	}
    }
    else {
	/*
	   if (z_orig != UNDEFZ) 
	   {
	   s = sunSlopeGeom->lum_C31_l * cos(-sungeom->timeAngle - sunSlopeGeom->longit_l) + sunSlopeGeom->lum_C33_l; 
	   }
	   else
	   {
	   s = sunVarGeom->sinSolarAltitude;
	   }
	 */
	s = sunSlopeGeom->lum_C31_l * cos(-sungeom->timeAngle - sunSlopeGeom->longit_l) + sunSlopeGeom->lum_C33_l;	/* Jenco */


    }

    if (s < 0)
	return 0.;
    return (s);
}



double brad(double sh, double *bh, struct SunGeometryVarDay *sunVarGeom,
	    struct SunGeometryVarSlope *sunSlopeGeom,
	    struct SolarRadVar *sunRadVar)
{
    double opticalAirMass, airMass2Linke, rayl, br;
    double drefract, temp1, temp2, h0refract;
    double elevationCorr;

    double locSolarAltitude;

    locSolarAltitude = sunVarGeom->solarAltitude;


    elevationCorr = exp(-sunVarGeom->z_orig / 8434.5);
    temp1 = 0.1594 + locSolarAltitude * (1.123 + 0.065656 * locSolarAltitude);
    temp2 = 1. + locSolarAltitude * (28.9344 + 277.3971 * locSolarAltitude);
    drefract = 0.061359 * temp1 / temp2;	/* in radians */
    h0refract = locSolarAltitude + drefract;
    opticalAirMass = elevationCorr / (sin(h0refract) +
				      0.50572 * pow(h0refract * rad2deg +
						    6.07995, -1.6364));
    airMass2Linke = 0.8662 * sunRadVar->linke;
    if (opticalAirMass <= 20.) {
	rayl = 1. / (6.6296 +
		     opticalAirMass * (1.7513 +
				       opticalAirMass * (-0.1202 +
							 opticalAirMass *
							 (0.0065 -
							  opticalAirMass *
							  0.00013))));
    }
    else {
	rayl = 1. / (10.4 + 0.718 * opticalAirMass);
    }
    *bh =
	sunRadVar->cbh * sunRadVar->G_norm_extra *
	sunVarGeom->sinSolarAltitude * exp(-rayl * opticalAirMass *
					   airMass2Linke);
    if (sunSlopeGeom->aspect != UNDEF && sunSlopeGeom->slope != 0.)
	br = *bh * sh / sunVarGeom->sinSolarAltitude;
    else
	br = *bh;

    return (br);
}

double brad_angle_loss(double sh, double *bh,
		       struct SunGeometryVarDay *sunVarGeom,
		       struct SunGeometryVarSlope *sunSlopeGeom,
		       struct SolarRadVar *sunRadVar)
{
    double p, opticalAirMass, airMass2Linke, rayl, br;
    double drefract, temp1, temp2, h0refract;

    double locSolarAltitude;

    locSolarAltitude = sunVarGeom->solarAltitude;


    p = exp(-sunVarGeom->z_orig / 8434.5);
    temp1 = 0.1594 + locSolarAltitude * (1.123 + 0.065656 * locSolarAltitude);
    temp2 = 1. + locSolarAltitude * (28.9344 + 277.3971 * locSolarAltitude);
    drefract = 0.061359 * temp1 / temp2;	/* in radians */
    h0refract = locSolarAltitude + drefract;
    opticalAirMass = p / (sin(h0refract) +
			  0.50572 * pow(h0refract * rad2deg + 6.07995,
					-1.6364));
    airMass2Linke = 0.8662 * sunRadVar->linke;
    if (opticalAirMass <= 20.)
	rayl =
	    1. / (6.6296 +
		  opticalAirMass * (1.7513 +
				    opticalAirMass * (-0.1202 +
						      opticalAirMass *
						      (0.0065 -
						       opticalAirMass *
						       0.00013))));
    else
	rayl = 1. / (10.4 + 0.718 * opticalAirMass);
    *bh =
	sunRadVar->cbh * sunRadVar->G_norm_extra *
	sunVarGeom->sinSolarAltitude * exp(-rayl * opticalAirMass *
					   airMass2Linke);
    if (sunSlopeGeom->aspect != UNDEF && sunSlopeGeom->slope != 0.)
	br = *bh * sh / sunVarGeom->sinSolarAltitude;
    else
	br = *bh;

    br *= (1 - exp(-sh / a_r)) * angular_loss_denom;

    return (br);
}

double brad_irr(double sh, double *bh,
		       struct SunGeometryVarDay *sunVarGeom,
		       struct SunGeometryVarSlope *sunSlopeGeom,
		       struct SolarRadVar *sunRadVar)
{
    double br;


	*bh = sunRadVar->cbh-sunRadVar->cdh; 
	
	if((*bh)>sunRadVar->G_norm_extra*sunVarGeom->sinSolarAltitude)
		{
		*bh=0.9*sunRadVar->G_norm_extra*sunVarGeom->sinSolarAltitude;
		sunRadVar->cdh=sunRadVar->cbh-(*bh);
		}
	if (sunSlopeGeom->aspect != UNDEF && sunSlopeGeom->slope != 0.)
		br = *bh * sh/ sunVarGeom->sinSolarAltitude;
	else
		br = *bh;

	return (br);
}

double brad_angle_loss_irr(double sh, double *bh,
		       struct SunGeometryVarDay *sunVarGeom,
		       struct SunGeometryVarSlope *sunSlopeGeom,
		       struct SolarRadVar *sunRadVar)
{
    double br;


	*bh = sunRadVar->cbh-sunRadVar->cdh; 
	if((*bh)>sunRadVar->G_norm_extra*sunVarGeom->sinSolarAltitude)
		{
		*bh=0.9*sunRadVar->G_norm_extra*sunVarGeom->sinSolarAltitude;
		sunRadVar->cdh=sunRadVar->cbh-(*bh);
		}
	if (sunSlopeGeom->aspect != UNDEF && sunSlopeGeom->slope != 0.)
		br = *bh * sh/ sunVarGeom->sinSolarAltitude;
	else
		br = *bh;

	br *= (1 - exp(-sh / a_r)) * angular_loss_denom;

    return (br);
}


void diff_skartveit(double sh, double ext_rad, double *diff_rad,
	    struct SunGeometryVarDay *sunVarGeom,
	    struct SunGeometryVarSlope *sunSlopeGeom,
	    struct SolarRadVar *sunRadVar)
{
	double clearnessIndex;
	double K, K1, d1;
	double ksi, d2g;

	double solarAngleDegrees;
	
	


	clearnessIndex=sunRadVar->cbh/ext_rad;
	
	
	if(clearnessIndex<0.2)
		{
		/* No direct radiation at all */
		*diff_rad=sunRadVar->cbh;
		return;
		}

	solarAngleDegrees=rad2deg*sunVarGeom->solarAltitude;

	K1=0.87-0.56*exp(-0.06*solarAngleDegrees);

	d1=0.15+0.43*exp(-0.06*solarAngleDegrees);
	
	
	if(clearnessIndex<1.09*K1)
		{
		K=0.5*(1.+sin(M_PI*((clearnessIndex-0.2)/(K1-0.2)-0.5)));
		d2g=1.-(1.-d1)*(0.27*sqrt(K)+0.73*K*K);
	
		}
	else
		{
		K=0.5*(1.+sin(M_PI*((1.09*clearnessIndex-0.2)/(K1-0.2)-0.5)));
		
		ksi=1.-(1.-d1)*(0.27*sqrt(K)+0.73*K*K);

		d2g=1.- (1.09*K1*(1.-ksi)/clearnessIndex);
	
		}
	*diff_rad=d2g*sunRadVar->cbh;
	return;
		
	
}



void diff_maxwell(double sh, double ext_rad, double *diff_rad,
	    struct SunGeometryVarDay *sunVarGeom,
	    struct SunGeometryVarSlope *sunSlopeGeom,
	    struct SolarRadVar *sunRadVar)
{
	double clearnessIndex, c2, c3;
	double acoef, bcoef, ccoef;
	double DNI;
	double Knc;
	
	
	double p, AM;
	double drefract, temp1, temp2, h0refract;

	double locSolarAltitude;

	clearnessIndex=sunRadVar->cbh/ext_rad;
	
	c2=clearnessIndex*clearnessIndex;
	c3=c2*clearnessIndex;
	
	locSolarAltitude = sunVarGeom->solarAltitude;


	p = exp(-sunVarGeom->z_orig / 8434.5);
	temp1 = 0.1594 + locSolarAltitude * (1.123 + 0.065656 * locSolarAltitude);
	temp2 = 1. + locSolarAltitude * (28.9344 + 277.3971 * locSolarAltitude);
	drefract = 0.061359 * temp1 / temp2;	/* in radians */
	h0refract = locSolarAltitude + drefract;
	AM = p / (sin(h0refract) +
			  0.50572 * pow(h0refract * rad2deg + 6.07995,
					-1.6364));
	
	AM=(AM<1.)?1.:AM;
	
	Knc=0.866-0.122*AM+0.0121*AM*AM-0.000653*AM*AM*AM+0.000014*AM*AM*AM*AM;
		
	if(clearnessIndex<=0.6)
		{
		acoef=0.512-1.56*clearnessIndex+2.286*c2	
			-2.222*c3;
		bcoef=0.37+0.962*clearnessIndex;
		ccoef=-0.280+0.932*clearnessIndex-2.048*c2;
		
		}
	else
		{
		acoef=-5.743+21.77*clearnessIndex-27.49*c2
			+11.56*c3;
		bcoef=41.4-118.5*clearnessIndex+66.05*c2+31.9*c3;
		ccoef=-47.01+184.2*clearnessIndex-222*c2+73.81*c3;
		
		}

	DNI=sunRadVar->G_norm_extra*(Knc-(acoef+bcoef*exp(AM*ccoef)));
		
	*diff_rad=sunRadVar->cbh-DNI*sunVarGeom->sinSolarAltitude;

	return;
	
}


double drad(double sh, double bh, double *rr,
	    struct SunGeometryVarDay *sunVarGeom,
	    struct SunGeometryVarSlope *sunSlopeGeom,
	    struct SolarRadVar *sunRadVar)
{
    double tn, fd, fx = 0., A1, A2, A3, A1b;
    double r_sky, kb, dr, gh, a_ln, ln, fg;
    double dh;
    double cosslope, sinslope;
    double locSinSolarAltitude;
    double locLinke;

    locLinke = sunRadVar->linke;
    locSinSolarAltitude = sunVarGeom->sinSolarAltitude;

    cosslope = cos(sunSlopeGeom->slope);
    sinslope = sin(sunSlopeGeom->slope);

    tn = -0.015843 + locLinke * (0.030543 + 0.0003797 * locLinke);
    A1b = 0.26463 + locLinke * (-0.061581 + 0.0031408 * locLinke);
    if (A1b * tn < 0.0022)
	A1 = 0.0022 / tn;
    else
	A1 = A1b;
    A2 = 2.04020 + locLinke * (0.018945 - 0.011161 * locLinke);
    A3 = -1.3025 + locLinke * (0.039231 + 0.0085079 * locLinke);

    fd = A1 + A2 * locSinSolarAltitude +
	A3 * locSinSolarAltitude * locSinSolarAltitude;
    dh = sunRadVar->cdh * sunRadVar->G_norm_extra * fd * tn;
    gh = bh + dh;
    if (sunSlopeGeom->aspect != UNDEF && sunSlopeGeom->slope != 0.) {
	kb = bh / (sunRadVar->G_norm_extra * locSinSolarAltitude);
	r_sky = (1. + cosslope) / 2.;
	a_ln = sunVarGeom->solarAzimuth - sunSlopeGeom->aspect;
	ln = a_ln;
	if (a_ln > M_PI)
	    ln = a_ln - pi2;
	else if (a_ln < -M_PI)
	    ln = a_ln + pi2;
	a_ln = ln;
	fg = sinslope - sunSlopeGeom->slope * cosslope -
	    M_PI * sin(0.5 * sunSlopeGeom->slope) * sin(0.5 *
							sunSlopeGeom->slope);
	if ((sunVarGeom->isShadow == 1) || sh <= 0.)
	    fx = r_sky + fg * 0.252271;
	else if (sunVarGeom->solarAltitude >= 0.1) {
	    fx = ((0.00263 - kb * (0.712 + 0.6883 * kb)) * fg + r_sky) * (1. -
									  kb)
		+ kb * sh / locSinSolarAltitude;
	}
	else if (sunVarGeom->solarAltitude < 0.1)
	    fx = ((0.00263 - 0.712 * kb - 0.6883 * kb * kb) * fg +
		  r_sky) * (1. - kb) + kb * sinslope * cos(a_ln) / (0.1 -
								    0.008 *
								    sunVarGeom->
								    solarAltitude);
	dr = dh * fx;
	/* refl. rad */
	*rr = sunRadVar->alb * gh * (1 - cosslope) / 2.;
    }
    else {			/* plane */
	dr = dh;
	*rr = 0.;
    }
    return (dr);
}

#define c2 -0.074

double drad_angle_loss(double sh, double bh, double *rr,
		       struct SunGeometryVarDay *sunVarGeom,
		       struct SunGeometryVarSlope *sunSlopeGeom,
		       struct SolarRadVar *sunRadVar)
{
    double dh;
    double tn, fd, fx = 0., A1, A2, A3, A1b;
    double r_sky, kb, dr, gh, a_ln, ln, fg;
    double cosslope, sinslope;

    double diff_coeff_angleloss;
    double refl_coeff_angleloss;
    double c1;
    double diff_loss_factor, refl_loss_factor;
    double locSinSolarAltitude;
    double locLinke;

    locSinSolarAltitude = sunVarGeom->sinSolarAltitude;
    locLinke = sunRadVar->linke;
    cosslope = cos(sunSlopeGeom->slope);
    sinslope = sin(sunSlopeGeom->slope);


    tn = -0.015843 + locLinke * (0.030543 + 0.0003797 * locLinke);
    A1b = 0.26463 + locLinke * (-0.061581 + 0.0031408 * locLinke);
    if (A1b * tn < 0.0022)
	A1 = 0.0022 / tn;
    else
	A1 = A1b;
    A2 = 2.04020 + locLinke * (0.018945 - 0.011161 * locLinke);
    A3 = -1.3025 + locLinke * (0.039231 + 0.0085079 * locLinke);

    fd = A1 + A2 * locSinSolarAltitude +
	A3 * locSinSolarAltitude * locSinSolarAltitude;
    dh = sunRadVar->cdh * sunRadVar->G_norm_extra * fd * tn;
    gh = bh + dh;
    if (sunSlopeGeom->aspect != UNDEF && sunSlopeGeom->slope != 0.) {
	kb = bh / (sunRadVar->G_norm_extra * locSinSolarAltitude);
	r_sky = (1. + cosslope) / 2.;
	a_ln = sunVarGeom->solarAzimuth - sunSlopeGeom->aspect;
	ln = a_ln;
	if (a_ln > M_PI)
	    ln = a_ln - pi2;
	else if (a_ln < -M_PI)
	    ln = a_ln + pi2;
	a_ln = ln;
	fg = sinslope - sunSlopeGeom->slope * cosslope -
	    M_PI * sin(sunSlopeGeom->slope / 2.) * sin(sunSlopeGeom->slope /
						       2.);
	if ((sunVarGeom->isShadow) || (sh <= 0.))
	    fx = r_sky + fg * 0.252271;
	else if (sunVarGeom->solarAltitude >= 0.1) {
	    fx = ((0.00263 - kb * (0.712 + 0.6883 * kb)) * fg + r_sky) * (1. -
									  kb)
		+ kb * sh / locSinSolarAltitude;
	}
	else if (sunVarGeom->solarAltitude < 0.1)
	    fx = ((0.00263 - 0.712 * kb - 0.6883 * kb * kb) * fg +
		  r_sky) * (1. - kb) + kb * sinslope * cos(a_ln) / (0.1 -
								    0.008 *
								    sunVarGeom->
								    solarAltitude);
	dr = dh * fx;
	/* refl. rad */
	*rr = sunRadVar->alb * gh * (1 - cosslope) / 2.;
    }
    else {			/* plane */
	dr = dh;
	*rr = 0.;
    }

    c1 = 4. / (3. * M_PI);
    diff_coeff_angleloss = sinslope
	+ (M_PI - sunSlopeGeom->slope - sinslope) / (1 + cosslope);

	if(cosslope==1.)
		refl_coeff_angleloss=0.;
	else
		{
    		refl_coeff_angleloss = sinslope
			+ (sunSlopeGeom->slope - sinslope) / (1 - cosslope);
		}

    diff_loss_factor
	= 1. - exp(-(c1 * diff_coeff_angleloss
		     + c2 * diff_coeff_angleloss * diff_coeff_angleloss)
		   / a_r);
    refl_loss_factor
	= 1. - exp(-(c1 * refl_coeff_angleloss
		     + c2 * refl_coeff_angleloss * refl_coeff_angleloss)
		   / a_r);

    dr *= diff_loss_factor;
    *rr *= refl_loss_factor;



    return (dr);
}


double drad_irr(double sh, double bh, double *rr,
		       struct SunGeometryVarDay *sunVarGeom,
		       struct SunGeometryVarSlope *sunSlopeGeom,
		       struct SolarRadVar *sunRadVar)
{
    double dh;
/*
    double tn, fx = 0., A1, A1b;
*/
	double fx=0.;
    double r_sky, kb, dr, gh, a_ln, ln, fg;
    double cosslope, sinslope;

    double locSinSolarAltitude;

    locSinSolarAltitude = sunVarGeom->sinSolarAltitude;
    cosslope = cos(sunSlopeGeom->slope);
    sinslope = sin(sunSlopeGeom->slope);

    
    dh = sunRadVar->cdh;
    gh = bh + dh;
    if (sunSlopeGeom->aspect != UNDEF && sunSlopeGeom->slope != 0.) {
	kb = bh / (sunRadVar->G_norm_extra * locSinSolarAltitude);
	r_sky = (1. + cosslope) / 2.;
	a_ln = sunVarGeom->solarAzimuth - sunSlopeGeom->aspect;
	ln = a_ln;
	if (a_ln > M_PI)
	    ln = a_ln - pi2;
	else if (a_ln < -M_PI)
	    ln = a_ln + pi2;
	a_ln = ln;
	fg = sinslope - sunSlopeGeom->slope * cosslope -
	    M_PI * sin(sunSlopeGeom->slope / 2.) * sin(sunSlopeGeom->slope /
						       2.);
	if ((sunVarGeom->isShadow) || (sh <= 0.))
	    fx = r_sky + fg * 0.252271;
	else if (sunVarGeom->solarAltitude >= 0.1) {
	    fx = ((0.00263 - kb * (0.712 + 0.6883 * kb)) * fg + r_sky) * (1. -
									  kb)
		+ kb * sh / locSinSolarAltitude;
	}
	else if (sunVarGeom->solarAltitude < 0.1)
	    fx = ((0.00263 - 0.712 * kb - 0.6883 * kb * kb) * fg +
		  r_sky) * (1. - kb) + kb * sinslope * cos(a_ln) / (0.1 -
								    0.008 *
								    sunVarGeom->
								    solarAltitude);
	dr = dh * fx;
	/* refl. rad */
	*rr = sunRadVar->alb * gh * (1 - cosslope) / 2.;
    }
    else {			/* plane */
	dr = dh;
	*rr = 0.;
    }


    return (dr);
}

double drad_angle_loss_irr(double sh, double bh, double *rr,
		       struct SunGeometryVarDay *sunVarGeom,
		       struct SunGeometryVarSlope *sunSlopeGeom,
		       struct SolarRadVar *sunRadVar)
{
    double dh;
/*
    double tn, fx = 0., A1, A1b;
*/
	double fx;
    double r_sky, kb, dr, gh, a_ln, ln, fg;
    double cosslope, sinslope;

    double diff_coeff_angleloss;
    double refl_coeff_angleloss;
    double c1;
    double diff_loss_factor, refl_loss_factor;
    double locSinSolarAltitude;

    locSinSolarAltitude = sunVarGeom->sinSolarAltitude;
    cosslope = cos(sunSlopeGeom->slope);
    sinslope = sin(sunSlopeGeom->slope);

/*
    tn = -0.015843 + locLinke * (0.030543 + 0.0003797 * locLinke);
    A1b = 0.26463 + locLinke * (-0.061581 + 0.0031408 * locLinke);
    if (A1b * tn < 0.0022)
	A1 = 0.0022 / tn;
    else
	A1 = A1b;
*/
    
	fx=0.;
    dh = sunRadVar->cdh;
    gh = bh + dh;
    if (sunSlopeGeom->aspect != UNDEF && sunSlopeGeom->slope != 0.) {
	kb = bh / (sunRadVar->G_norm_extra * locSinSolarAltitude);
	r_sky = (1. + cosslope) / 2.;
	a_ln = sunVarGeom->solarAzimuth - sunSlopeGeom->aspect;
	ln = a_ln;
	if (a_ln > M_PI)
	    ln = a_ln - pi2;
	else if (a_ln < -M_PI)
	    ln = a_ln + pi2;
	a_ln = ln;
	fg = sinslope - sunSlopeGeom->slope * cosslope -
	    M_PI * sin(sunSlopeGeom->slope / 2.) * sin(sunSlopeGeom->slope /
						       2.);
	if ((sunVarGeom->isShadow) || (sh <= 0.))
	    fx = r_sky + fg * 0.252271;
	else if (sunVarGeom->solarAltitude >= 0.1) {
	    fx = ((0.00263 - kb * (0.712 + 0.6883 * kb)) * fg + r_sky) * (1. -
									  kb)
		+ kb * sh / locSinSolarAltitude;
	}
	else if (sunVarGeom->solarAltitude < 0.1)
	    fx = ((0.00263 - 0.712 * kb - 0.6883 * kb * kb) * fg +
		  r_sky) * (1. - kb) + kb * sinslope * cos(a_ln) / (0.1 -
								    0.008 *
								    sunVarGeom->
								    solarAltitude);
	dr = dh * fx;
	/* refl. rad */
	*rr = sunRadVar->alb * gh * (1 - cosslope) / 2.;
    }
    else {			/* plane */
	dr = dh;
	*rr = 0.;
    }

    c1 = 4. / (3. * M_PI);
    diff_coeff_angleloss = sinslope
	+ (M_PI - sunSlopeGeom->slope - sinslope) / (1 + cosslope);
	if(cosslope==1.)
		refl_coeff_angleloss=0.;
	else
		{
    		refl_coeff_angleloss = sinslope
			+ (sunSlopeGeom->slope - sinslope) / (1 - cosslope);
		}

    diff_loss_factor
	= 1. - exp(-(c1 * diff_coeff_angleloss
		     + c2 * diff_coeff_angleloss * diff_coeff_angleloss)
		   / a_r);
    refl_loss_factor
	= 1. - exp(-(c1 * refl_coeff_angleloss
		     + c2 * refl_coeff_angleloss * refl_coeff_angleloss)
		   / a_r);

    dr *= diff_loss_factor;
    *rr *= refl_loss_factor;



    return (dr);
}

