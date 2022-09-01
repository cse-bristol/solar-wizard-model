
/*******************************************************************************
r.pv: This program is based on r.sun, by Jaro Hofierka. It calculates 
	photovoltaic output based on incoming solar irradiance and ambient
	temperature.
(C) 2004 Copyright Thomas Huld, JRC Ispra, 21020 Ispra, Italy
	mail: Thomas.Huld@jrc.it
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
#include <stdlib.h>
#include <math.h>
#include <grass/gis.h>
#include <grass/raster.h>
#include <grass/gprojects.h>
#include "rsunglobals.h"
#include "sunradstruct.h"
#include "local_proto.h"

#define HOURANGLE M_PI/12.
#define NUM_PARTITIONS "10"
#define UNDEF    0.             /* undefined value for terrain aspect */
#define UNDEFZ   -9999.         /* internal undefined value for NULL */
#define SKIP    "1"
#define BIG      1.e20
#define IBIG     32767
#define EPS      1.e-4
#define LINKE    "3.0"
#define SLOPE    "0.0"
#define ASPECT   "270"
#define ALB      "0.2"
#define STEP     "0.5"
#define BSKY      1.0
#define DSKY      1.0
#define DIST     "1.0"

#define SCALING_FACTOR 150.
const double invScale = 1. / SCALING_FACTOR;

const double pihalf = M_PI * 0.5;

const double pi2 = M_PI * 2.;

const double deg2rad = M_PI / 180.;

const double rad2deg = 180. / M_PI;


#define AMAX1(arg1, arg2) ((arg1) >= (arg2) ? (arg1) : (arg2))
#define AMIN1(arg1, arg2) ((arg1) <= (arg2) ? (arg1) : (arg2))
#define DISTANCE1(x1, x2, y1, y2) (sqrt((x1 - x2)*(x1 - x2) + (y1 - y2)*(y1 - y2)))


char *str_step;


char *elevin;

char *aspin;

char *slopein;

char *civiltime = NULL;

char *linkein = NULL;

char *albedo = NULL;

char *latin = NULL;

char **coeftemp = NULL;

char *coefwind = NULL;

char *coefbh = NULL;

char *coefdh = NULL;

char *longin = NULL;

char *horizon = NULL;

char *beam_rad = NULL;

char *diff_rad = NULL;

char *refl_rad = NULL;

char *glob_pow = NULL;

char *mod_temp = NULL;

char *modelparameters = NULL;

char *mapset = NULL;

char *per;

char *shade;

char mapname[1024];

struct Cell_head cellhd;

struct pj_info iproj;

struct pj_info oproj;

struct History hist;

int initEfficiencyCoeffs(char *filename, int useWind);

int INPUT_part(int offset, double *zmax);

int OUTGR(void);

int min(int, int);

int max(int, int);

double powerDeficit(double temp, double rad);

double temperature(double *coeffs, double time);

void cube(int, int);

void (*func) (int, int);

int useTemperature_global = 0;

int useTemperature()
{
    return useTemperature_global;
}

void setUseTemperature(int val)
{
    useTemperature_global = val;
}

int useWind_global = 0;

int useWind()
{
    return useWind_global;
}

void setUseWind(int val)
{
    useWind_global = val;
}

int num_temperatures;

int numTemperatures()
{
    return num_temperatures;
}

void setNumTemperatures(int val)
{
    num_temperatures = val;
}


void cube(int jmin, int imin)
{
}


void joules2(double *totpower, double *modtemp,
             struct SunGeometryConstDay *sunGeom,
             struct SunGeometryVarDay *sunVarGeom,
             struct SunGeometryVarSlope *sunSlopeGeom,
             struct SolarRadVar *sunRadVar,
             struct GridGeometry *gridGeom,
             float *temperature, double *wcoeffs,
             unsigned char *horizonpointer,
             double latitude, double longitude,
             BeamRadFunc bRadFunc, DiffRadFunc dRadFunc);


void calculate(int angleloss, double singleSlope, double singleAspect,
               struct SolarRadVar globalRadValues,
               struct GridGeometry gridGeom);
double com_sol_const(int);

double com_declin(int);

int shd, typ, n, m, ip, jp;

int highIrr;

int d, day;

int saveMemory, numPartitions = 1;

int shadowoffset = 0;

int arrayNumInt;

float **z = NULL, **o = NULL, **s = NULL, **li = NULL, **a = NULL, **la =
    NULL, **longitArray, **cbhr = NULL, **cdhr =
    NULL, *tempdata, **windCoeff0, **windCoeff1, **windCoeff2, **windCoeff3;
double op, dp;

double invstepx, invstepy;

double sr_min = 24., sr_max = 0., ss_min = 24., ss_max = 0.;

double offsetx = 0.5, offsety = 0.5;

float **lumcl, **beam, **diff, **refl, **globrad, **modtemp_rast;

unsigned char *horizonarray = NULL;

double civilTime;

double timeOffset = 0., totOffsetTime, longitTime = 0.;

double xmin, xmax, ymin, ymax;

double zmult = 1.0, declin, step, dist;

double li_max = 0., li_min = 100., al_max = 0., al_min = 1.0, la_max = -90.,
    la_min = 90.;
char *tt, *lt;

double horizonStep;

double ltime, tim, timo, declination;

double beam_e, diff_e, refl_e, insol_t;

double TOLER;


double temperature(double *coeffs, double time)
{
    return coeffs[0] * time * time * time + coeffs[1] * time * time +
        coeffs[2] * time + coeffs[3];
}


double temperatureInterpolate(float *temperature, float time, float longitude)
{
    int prevslot, nextslot;

    float timeFrac;

    double locTime = time - longitude * rad2deg / 15.;

    double timeInterval = 24. / numTemperatures();

    if (locTime < 0.)
        locTime += 24;
    if (locTime > 24)
        locTime -= 24;

    prevslot = (int)(lrint(floor(locTime)) / timeInterval);
    nextslot = prevslot + 1;
    if (nextslot == numTemperatures())
        nextslot = 0;

    timeFrac = locTime - timeInterval * prevslot;

    return temperature[prevslot] +
        (timeFrac / timeInterval) * (temperature[nextslot] -
                                     temperature[prevslot]);


}


int main(int argc, char *argv[])
{
    int angleloss = 0;

    double singleSlope;

    double singleAspect;

    struct GridGeometry gridGeom;

    struct SolarRadVar globalRadValues;

    struct GModule *module;

    struct
    {
        struct Option *elevin, *aspin, *aspect, *slopein, *slope, *linkein,
            *lin, *albedo, *longin, *alb, *latin, *coefbh, *coefdh, *beam_rad,
            *coeffTemp, *coeffWind, *horizon, *horizonstep, *diff_rad,
            *refl_rad, *glob_pow, *mod_temp, *day, *step, *declin, *ltime,
            *dist, *numPartitions, *civilTime, *modelparameters;
    }
    parm;

    struct
    {
        struct Flag *shade, *saveMemory, *highIrradiance, *angleLoss;
    }
    flag;

    /* Set a constant value */

    setAngularLossDenominator();


    G_gisinit(argv[0]);
    module = G_define_module();

    module->description =
        "Computes photovoltaic power output raster map, based on direct (beam), diffuse "
        "and reflected solar irradiation as well as ambient temperature "
        "for a given day, latitude, surface and atmospheric conditions. Solar "
        "parameters (e.g. sunrise, sunset times, declination, extraterrestrial "
        "irradiance, daylight length) are saved in a local text file. "
        "Alternatively, a local time can be specified to compute solar "
        "incidence angle and/or irradiance raster maps. The shadowing effect of "
        "the topography is optionally incorporated. ";

    G_get_set_window(&cellhd);
    gridGeom.stepx = cellhd.ew_res;
    gridGeom.stepy = cellhd.ns_res;
    invstepx = 1. / gridGeom.stepx;
    invstepy = 1. / gridGeom.stepy;
    n /*n_cols */  = cellhd.cols;
    m /*n_rows */  = cellhd.rows;
    xmin = cellhd.west;
    ymin = cellhd.south;
    xmax = cellhd.east;
    ymax = cellhd.north;
    gridGeom.deltx = fabs(cellhd.east - cellhd.west);
    gridGeom.delty = fabs(cellhd.north - cellhd.south);

    parm.elevin = G_define_option();
    parm.elevin->key = "elevation";
    parm.elevin->type = TYPE_STRING;
    parm.elevin->required = YES;
    parm.elevin->gisprompt = "old,cell,raster";
    parm.elevin->description = "Name of the elevation raster file";

    parm.aspin = G_define_option();
    parm.aspin->key = "aspect";
    parm.aspin->type = TYPE_STRING;
    parm.aspin->required = NO;
    parm.aspin->gisprompt = "old,cell,raster";
    parm.aspin->description = "Name of the aspect raster file";

    parm.aspect = G_define_option();
    parm.aspect->key = "aspect_value";
    parm.aspect->type = TYPE_DOUBLE;
    parm.aspect->answer = ASPECT;
    parm.aspect->required = NO;
    parm.aspect->description =
        "A single value of the orientation (aspect), 270 is south";

    parm.slopein = G_define_option();
    parm.slopein->key = "slope";
    parm.slopein->type = TYPE_STRING;
    parm.slopein->required = NO;
    /*      parm.slopein->gisprompt = "old,cell,raster";
     */
    parm.slopein->description = "Name of the slope raster file";

    parm.slope = G_define_option();
    parm.slope->key = "slope_value";
    parm.slope->type = TYPE_DOUBLE;
    parm.slope->answer = SLOPE;
    parm.slope->required = NO;
    parm.slope->description = "A single value of inclination (slope)";

    parm.linkein = G_define_option();
    parm.linkein->key = "linke";
    parm.linkein->type = TYPE_STRING;
    parm.linkein->required = NO;
    parm.linkein->gisprompt = "old,cell,raster";
    parm.linkein->description =
        "Name of the Linke turbidity coefficient raster file";

    parm.lin = G_define_option();
    parm.lin->key = "linke_value";
    parm.lin->type = TYPE_DOUBLE;
    parm.lin->answer = LINKE;
    parm.lin->required = NO;
    parm.lin->description =
        "A single value of the Linke turbidity coefficient";

    parm.albedo = G_define_option();
    parm.albedo->key = "albedo";
    parm.albedo->type = TYPE_STRING;
    parm.albedo->required = NO;
    parm.albedo->gisprompt = "old,cell,raster";
    parm.albedo->description = "Name of the albedo coefficient raster file";

    parm.alb = G_define_option();
    parm.alb->key = "albedo_value";
    parm.alb->type = TYPE_DOUBLE;
    parm.alb->answer = ALB;
    parm.alb->required = NO;
    parm.alb->description = "A single value of the albedo coefficient";

    parm.latin = G_define_option();
    parm.latin->key = "lat";
    parm.latin->type = TYPE_STRING;
    parm.latin->required = NO;
    parm.latin->gisprompt = "old,cell,raster";
    parm.latin->description = "Name of the latitude raster file";


    parm.longin = G_define_option();
    parm.longin->key = "long";
    parm.longin->type = TYPE_STRING;
    parm.longin->required = NO;
    parm.longin->gisprompt = "old,cell,raster";
    parm.longin->description = "Name of the longitude raster file";


    parm.coefbh = G_define_option();
    parm.coefbh->key = "coefbh";
    parm.coefbh->type = TYPE_STRING;
    parm.coefbh->required = NO;
    parm.coefbh->gisprompt = "old,cell,raster";
    parm.coefbh->description = "The real-sky beam radiation coefficient file";

    parm.coefdh = G_define_option();
    parm.coefdh->key = "coefdh";
    parm.coefdh->type = TYPE_STRING;
    parm.coefdh->required = NO;
    parm.coefdh->gisprompt = "old,cell,raster";
    parm.coefdh->description =
        "The real-sky diffuse radiation coefficient file";


    parm.horizon = G_define_option();
    parm.horizon->key = "horizon_basename";
    parm.horizon->type = TYPE_STRING;
    parm.horizon->required = NO;
    parm.horizon->gisprompt = "old,cell,raster";
    parm.horizon->description = "The horizon information file prefix";

    parm.horizonstep = G_define_option();
    parm.horizonstep->key = "horizon_step";
    parm.horizonstep->type = TYPE_DOUBLE;
    parm.horizonstep->required = NO;
    parm.horizonstep->description =
        "Angle step size for the horizon information (degrees)";

    parm.beam_rad = G_define_option();
    parm.beam_rad->key = "beam_rad";
    parm.beam_rad->type = TYPE_STRING;
    parm.beam_rad->required = NO;
    parm.beam_rad->gisprompt = "new,cell,raster";
    parm.beam_rad->description =
        "Output direct (beam) irradiance/irradiation file (raster)";

    parm.diff_rad = G_define_option();
    parm.diff_rad->key = "diff_rad";
    parm.diff_rad->type = TYPE_STRING;
    parm.diff_rad->required = NO;
    parm.diff_rad->gisprompt = "new,cell,raster";
    parm.diff_rad->description =
        "Output diffuse irradiance/irradiation file (raster)";

    parm.refl_rad = G_define_option();
    parm.refl_rad->key = "refl_rad";
    parm.refl_rad->type = TYPE_STRING;
    parm.refl_rad->required = NO;
    parm.refl_rad->gisprompt = "new,cell,raster";
    parm.refl_rad->description =
        "Output reflected irradiance/irradiation file (raster)";

    parm.glob_pow = G_define_option();
    parm.glob_pow->key = "glob_pow";
    parm.glob_pow->type = TYPE_STRING;
    parm.glob_pow->required = NO;
    parm.glob_pow->gisprompt = "new,cell,raster";
    parm.glob_pow->description =
        "Output global (total) irradiance/irradiation file (raster)";

    parm.mod_temp = G_define_option();
    parm.mod_temp->key = "mod_temp";
    parm.mod_temp->type = TYPE_STRING;
    parm.mod_temp->required = NO;
    parm.mod_temp->gisprompt = "new,cell,raster";
    parm.mod_temp->description = "Output Module temperature (raster)";


    parm.coeffTemp = G_define_option();
    parm.coeffTemp->key = "temperatures";
    parm.coeffTemp->type = TYPE_STRING;
    parm.coeffTemp->required = YES;
    parm.coeffTemp->multiple = YES;
    parm.coeffTemp->gisprompt = "old,cell,raster";
    parm.coeffTemp->description = "Name(s) of the temperature raster map(s)";

    parm.coeffWind = G_define_option();
    parm.coeffWind->key = "coeffwind";
    parm.coeffWind->type = TYPE_STRING;
    parm.coeffWind->required = NO;
    parm.coeffWind->gisprompt = "old,cell,raster";
    parm.coeffWind->description = "Base name of the wind coefficients files";

    parm.day = G_define_option();
    parm.day->key = "day";
    parm.day->type = TYPE_INTEGER;
    parm.day->required = YES;
    parm.day->description = "No. of day of the year (1-365)";

    parm.step = G_define_option();
    parm.step->key = "step";
    parm.step->type = TYPE_DOUBLE;
    parm.step->answer = STEP;
    parm.step->required = NO;
    parm.step->description = "Time step computing all-day radiation";

    parm.declin = G_define_option();
    parm.declin->key = "declin";
    parm.declin->type = TYPE_DOUBLE;
    parm.declin->required = NO;
    parm.declin->description =
        "Required declination value (overriding the internal value)";

    parm.ltime = G_define_option();
    parm.ltime->key = "time";
    parm.ltime->type = TYPE_DOUBLE;
    /*          parm.ltime->answer = TIME; */
    parm.ltime->required = NO;
    parm.ltime->description = "Local (solar) time [decimal hours]";


    parm.dist = G_define_option();
    parm.dist->key = "dist";
    parm.dist->type = TYPE_DOUBLE;
    parm.dist->answer = DIST;
    parm.dist->required = NO;
    parm.dist->description = "Sampling distance step coefficient (0.5-1.5)";

    parm.numPartitions = G_define_option();
    parm.numPartitions->key = "numpartitions";
    parm.numPartitions->type = TYPE_INTEGER;
    parm.numPartitions->answer = NUM_PARTITIONS;
    parm.numPartitions->required = NO;
    parm.numPartitions->description =
        "Read the input files in this number of chunks";

    parm.civilTime = G_define_option();
    parm.civilTime->key = "civiltime";
    parm.civilTime->type = TYPE_DOUBLE;
    parm.civilTime->required = NO;
    parm.civilTime->description =
        "(optional) The civil time zone value, if none, the time will be local solar time";

    parm.modelparameters = G_define_option();
    parm.modelparameters->key = "modelparameters";
    parm.modelparameters->type = TYPE_STRING;
    parm.modelparameters->required = NO;
    parm.modelparameters->description =
        "(optional) Name of the file with the parameters for the power rating model (file should be in local directory)";


    flag.angleLoss = G_define_flag();
    flag.angleLoss->key = 'a';
    flag.angleLoss->description =
        "Do you want to include the effect of shallow angle reflectivity (y/n)";


    flag.shade = G_define_flag();
    flag.shade->key = 's';
    flag.shade->description =
        "Do you want to incorporate the shadowing effect of terrain (y/n)";

    flag.saveMemory = G_define_flag();
    flag.saveMemory->key = 'm';
    flag.saveMemory->description =
        "Do you want to use the low-memory version of the program (y/n)";

    flag.highIrradiance = G_define_flag();
    flag.highIrradiance->key = 'i';
    flag.highIrradiance->description =
        "Do you want to use clear-sky irradiance for calculating efficiency (y/n)";


    if (G_parser(argc, argv))
        exit(1);

    shd = flag.shade->answer;
    setUseShadow(flag.shade->answer);

    saveMemory = flag.saveMemory->answer;
    highIrr = flag.highIrradiance->answer;
    angleloss = flag.angleLoss->answer;
    civiltime = parm.civilTime->answer;


    elevin = parm.elevin->answer;
    aspin = parm.aspin->answer;
    slopein = parm.slopein->answer;
    linkein = parm.linkein->answer;
    albedo = parm.albedo->answer;
    latin = parm.latin->answer;

    modelparameters = parm.modelparameters->answer;


    if (civiltime != NULL) {
        setUseCivilTime(1);
        longin = parm.longin->answer;
        sscanf(parm.civilTime->answer, "%lf", &civilTime);

        // Normalize if somebody should be weird enough to give more than +- 12 
        // hours offset.

        if (civilTime < -12.) {
            civilTime += 24.;
        }
        else if (civilTime > 12.) {
            civilTime -= 24;
        }

    }
    else {
        setUseCivilTime(0);
    }

    coefbh = parm.coefbh->answer;
    coefdh = parm.coefdh->answer;
    horizon = parm.horizon->answer;
    setUseHorizonData(horizon != NULL);
    beam_rad = parm.beam_rad->answer;
    diff_rad = parm.diff_rad->answer;
    refl_rad = parm.refl_rad->answer;
    glob_pow = parm.glob_pow->answer;
    mod_temp = parm.mod_temp->answer;
    coeftemp = parm.coeffTemp->answers;
    coefwind = parm.coeffWind->answer;

    if (coeftemp != NULL)
        setUseTemperature(1);
    if (coefwind != NULL)
        setUseWind(1);

    initEfficiencyCoeffs(modelparameters, useWind());

    sscanf(parm.day->answer, "%d", &day);
    sscanf(parm.step->answer, "%lf", &step);

    tt = parm.ltime->answer;
    if (parm.ltime->answer != NULL) {
        fprintf(stdout,
                "Mode 1: instantaneous solar incidence angle & irradiance using a set local time\n");
        fflush(stdout);
        sscanf(parm.ltime->answer, "%lf", &timo);
    }
    else {
        fprintf(stdout, "Mode 2: integrated daily irradiation\n");
        fflush(stdout);
    }


    if (parm.horizonstep->answer != NULL) {
        if (sscanf(parm.horizonstep->answer, "%lf", &horizonStep) != 1) {
            G_fatal_error("Error reading horizon step size");

        }
        str_step = parm.horizonstep->answer;
        setHorizonInterval(deg2rad * horizonStep);
    }




    if (parm.linkein->answer == NULL)
        sscanf(parm.lin->answer, "%lf", &(globalRadValues.linke));
    if (parm.albedo->answer == NULL)
        sscanf(parm.alb->answer, "%lf", &(globalRadValues.alb));
    if (parm.slopein->answer == NULL)
        sscanf(parm.slope->answer, "%lf", &singleSlope);
    singleSlope *= deg2rad;

    if (parm.aspin->answer == NULL)
        sscanf(parm.aspect->answer, "%lf", &singleAspect);
    singleAspect *= deg2rad;

    if (parm.coefbh->answer == NULL)
        globalRadValues.cbh = BSKY;
    if (parm.coefdh->answer == NULL)
        globalRadValues.cdh = DSKY;
    sscanf(parm.dist->answer, "%lf", &dist);

    if (parm.numPartitions->answer != NULL) {
        sscanf(parm.numPartitions->answer, "%d", &numPartitions);
        if (useShadow() && (!useHorizonData()) && (numPartitions != 1)) {
            /* If you calculate shadows on the fly, the number of partitions
               must be one.
             */
            G_fatal_error
                ("If you use -s and no horizon rasters, numpartitions must be =1");

        }
    }

    gridGeom.stepxy = dist * 0.5 * (gridGeom.stepx + gridGeom.stepy);
    TOLER = gridGeom.stepxy * EPS;



    if (parm.declin->answer == NULL)
        declination = com_declin(day);
    else {
        sscanf(parm.declin->answer, "%lf", &declin);
        declination = -declin;
    }



    if (tt != 0) {
        /* Shadow for just one time during the day */
        if (horizon == NULL) {
            arrayNumInt = 1;
        }
        else {
            arrayNumInt = (int)(360. / horizonStep);

        }
    }
    else {
        /*        Number of bytes holding the horizon information */
        arrayNumInt = (int)(360. / horizonStep);

    }

    if (tt != NULL) {

        tim = (timo - 12) * 15;
        /* converting to degrees */
        /* Jenco (12-timeAngle) * 15 */
        if (tim < 0)
            tim += 360;
        tim = deg2rad * tim;
        /* conv. to radians */
    }

    /* Set up parameters for projection to lat/long if necessary */


    struct Key_Value *in_proj_info, *in_unit_info;

    struct Key_Value *out_proj_info, *out_unit_info;

    if ((in_proj_info = G_get_projinfo()) == NULL)
        G_fatal_error
            ("Can't get projection info of current location: please set latitude via 'lat' or 'latin' option!");

    if ((in_unit_info = G_get_projunits()) == NULL)
        G_fatal_error("Can't get projection units of current location");

    if (pj_get_kv(&iproj, in_proj_info, in_unit_info) < 0)
        G_fatal_error("Can't get projection key values of current location");

    /* Set output projection to latlong w/ same ellipsoid */
    oproj.zone = 0;
    oproj.meters = 1.;
    sprintf(oproj.proj, "ll");
    if ((oproj.pj = pj_latlong_from_proj(iproj.pj)) == NULL)
        G_fatal_error("Unable to set up lat/long projection parameters");


    G_free_key_value(in_proj_info);
    G_free_key_value(in_unit_info);

/**********end of parser - ******************************/

    calculate(angleloss, singleSlope, singleAspect, globalRadValues,
              gridGeom);
    OUTGR();

    return 1;
}


int INPUT_part(int offset, double *zmax)
{
    int h;

    int finalRow, rowrevoffset;

    int numRows;

    int decimals;

    int loc_num_temperatures;

    double angle_deg = 0.;

    FCELL *cell1 = NULL, *cell2 = NULL;

    FCELL *cell3 = NULL, *cell4 = NULL, *cell5 = NULL, *cell6 = NULL, *cell7 =
        NULL;
    FCELL *rast1 = NULL, *rast2 = NULL, *rasttemp = NULL, *rast4 =
        NULL, *rast5 = NULL, *rast6 = NULL;
    FCELL *rastw1 = NULL, *rastw2 = NULL, *rastw3 = NULL, *rastw4 = NULL;

    static FCELL **horizonbuf;

    unsigned char *horizonpointer;

    int fd1 = -1, fd2 = -1, fd3 = -1, fd4 = -1, fd5 = -1, fd6 = -1, fd7 =
        -1, row, row_rev;
    int ft1, ft2, ft3, ft4;

    int fw1, fw2, fw3, fw4;

    static int *fd_shad;

    int fr1, fr2;

    int l, i, j;

    char tempcoefname[256];

    char *shad_filename;

    finalRow = m - offset - m / numPartitions;
    if (finalRow < 0) {
        finalRow = 0;
    }

    numRows = m / numPartitions;

    cell1 = Rast_allocate_f_buf();

    if (z == NULL) {
        z = (float **)malloc(sizeof(float *) * (numRows));


        for (l = 0; l < numRows; l++) {
            z[l] = (float *)malloc(sizeof(float) * (n));
        }
    }




    /*
       if((mapset=G_find_cell(elevin,""))==NULL)
       printf("cell file not found\n");
     */


    fd1 = Rast_open_old(elevin, "");

    if (slopein != NULL) {
        cell3 = Rast_allocate_f_buf();
        if (s == NULL) {
            s = (float **)malloc(sizeof(float *) * (numRows));

            for (l = 0; l < numRows; l++) {
                s[l] = (float *)malloc(sizeof(float) * (n));
            }

        }
        fd3 = Rast_open_old(slopein, "");

    }

    if (aspin != NULL) {
        cell2 = Rast_allocate_f_buf();

        if (o == NULL) {
            o = (float **)malloc(sizeof(float *) * (numRows));

            for (l = 0; l < numRows; l++) {
                o[l] = (float *)malloc(sizeof(float) * (n));
            }
        }

        fd2 = Rast_open_old(aspin, "");

    }


    if (linkein != NULL) {
        cell4 = Rast_allocate_f_buf();
        if (li == NULL) {
            li = (float **)malloc(sizeof(float *) * (numRows));
            for (l = 0; l < numRows; l++)
                li[l] = (float *)malloc(sizeof(float) * (n));

        }

        fd4 = Rast_open_old(linkein, "");
    }

    if (albedo != NULL) {
        cell5 = Rast_allocate_f_buf();
        if (a == NULL) {
            a = (float **)malloc(sizeof(float *) * (numRows));
            for (l = 0; l < numRows; l++)
                a[l] = (float *)malloc(sizeof(float) * (n));
        }

        fd5 = Rast_open_old(albedo, "");
    }

    if (latin != NULL) {
        cell6 = Rast_allocate_f_buf();
        if (la == NULL) {
            la = (float **)malloc(sizeof(float *) * (numRows));
            for (l = 0; l < numRows; l++)
                la[l] = (float *)malloc(sizeof(float) * (n));
        }

        fd6 = Rast_open_old(latin, "");
    }

    if (longin != NULL) {
        cell7 = Rast_allocate_f_buf();
        longitArray = (float **)malloc(sizeof(float *) * (m));
        for (l = 0; l < m; l++)
            longitArray[l] = (float *)malloc(sizeof(float) * (n));


        fd7 = Rast_open_old(longin, "");
    }



    if (coefbh != NULL) {
        rast1 = Rast_allocate_f_buf();

        if (cbhr == NULL) {
            cbhr = (float **)malloc(sizeof(float *) * (numRows));
            for (l = 0; l < numRows; l++)
                cbhr[l] = (float *)malloc(sizeof(float) * (n));
        }

        fr1 = Rast_open_old(coefbh, mapset);
    }

    if (coefdh != NULL) {
        rast2 = Rast_allocate_f_buf();
        if (cdhr == NULL) {
            cdhr = (float **)malloc(sizeof(float *) * (numRows));
            for (l = 0; l < numRows; l++)
                cdhr[l] = (float *)malloc(sizeof(float) * (n));
        }

        fr2 = Rast_open_old(coefdh, mapset);
    }

    if (coeftemp != NULL) {

        for (i = 0; coeftemp[i]; i++) ;
        setNumTemperatures(i);
        loc_num_temperatures = i;


        rasttemp = Rast_allocate_f_buf();
        if (tempdata == NULL) {
            tempdata =
                (float *)malloc(sizeof(float *) *
                                (numRows * n * num_temperatures));

        }


        for (i = 0; i < loc_num_temperatures; i++) {


            ft1 = Rast_open_old(coeftemp[i], "");
            for (row = m - offset - 1; row >= finalRow; row--) {
                row_rev = m - row - 1;
                rowrevoffset = row_rev - offset;
                Rast_get_f_row(ft1, rasttemp, row);
                for (j = 0; j < n; j++) {
                    if (!Rast_is_f_null_value(rasttemp + j))
                        tempdata[(rowrevoffset * n +
                                  j) * loc_num_temperatures + i] =
                            (float)rasttemp[j];
                    else
                        tempdata[(rowrevoffset * n +
                                  j) * loc_num_temperatures + i] = UNDEFZ;
                }
            }
            Rast_close(ft1);
        }
        G_free(rasttemp);
    }

    if (coefwind != NULL) {
        rastw1 = Rast_allocate_f_buf();
        if (windCoeff0 == NULL) {
            windCoeff0 = (float **)malloc(sizeof(float *) * (numRows));
            for (l = 0; l < numRows; l++)
                windCoeff0[l] = (float *)malloc(sizeof(float) * (n));

        }

        sprintf(tempcoefname, "%s_0", coefwind);


        fw1 = Rast_open_old(tempcoefname, "");


        rastw2 = Rast_allocate_f_buf();
        if (windCoeff1 == NULL) {
            windCoeff1 = (float **)malloc(sizeof(float *) * (numRows));
            for (l = 0; l < numRows; l++)
                windCoeff1[l] = (float *)malloc(sizeof(float) * (n));

        }

        sprintf(tempcoefname, "%s_1", coefwind);


        fw2 = Rast_open_old(tempcoefname, "");


        rastw3 = Rast_allocate_f_buf();
        if (windCoeff2 == NULL) {
            windCoeff2 = (float **)malloc(sizeof(float *) * (numRows));
            for (l = 0; l < numRows; l++)
                windCoeff2[l] = (float *)malloc(sizeof(float) * (n));

        }

        sprintf(tempcoefname, "%s_2", coefwind);

        fw3 = Rast_open_old(tempcoefname, "");


        rastw4 = Rast_allocate_f_buf();
        if (windCoeff3 == NULL) {
            windCoeff3 = (float **)malloc(sizeof(float *) * (numRows));
            for (l = 0; l < numRows; l++)
                windCoeff3[l] = (float *)malloc(sizeof(float) * (n));

        }

        sprintf(tempcoefname, "%s_3", coefwind);


        fw4 = Rast_open_old(tempcoefname, "");

    }


    if (useHorizonData()) {
        if (horizonarray == NULL) {
            horizonarray =
                (unsigned char *)malloc(sizeof(char) * arrayNumInt * numRows *
                                        n);

            horizonbuf = (FCELL **) malloc(sizeof(FCELL *) * arrayNumInt);
            fd_shad = (int *)malloc(sizeof(int) * arrayNumInt);
        }
        decimals = G_get_num_decimals(str_step);
        angle_deg = 0;
        for (i = 0; i < arrayNumInt; i++) {
            horizonbuf[i] = Rast_allocate_f_buf();
            shad_filename = G_generate_basename(horizon, angle_deg,
                                                3, decimals);
            fd_shad[i] = Rast_open_old(shad_filename, "");
            angle_deg += horizonStep;
            G_free(shad_filename);
        }
    }


    if (useHorizonData()) {


        for (i = 0; i < arrayNumInt; i++) {
            for (row = m - offset - 1; row >= finalRow; row--) {

                row_rev = m - row - 1;
                rowrevoffset = row_rev - offset;
                Rast_get_f_row(fd_shad[i], horizonbuf[i], row);
                horizonpointer =
                    horizonarray + arrayNumInt * n * rowrevoffset;
                for (j = 0; j < n; j++) {

                    horizonpointer[i] = (char)(rint(SCALING_FACTOR
                                                    * fmin(horizonbuf[i][j],
                                                           256 * invScale)));
                    horizonpointer += arrayNumInt;

                }
            }
        }


    }





    for (row = m - offset - 1; row >= finalRow; row--) {
        Rast_get_f_row(fd1, cell1, row);
        if (aspin != NULL)
            Rast_get_f_row(fd2, cell2, row);
        if (slopein != NULL)
            Rast_get_f_row(fd3, cell3, row);
        if (linkein != NULL)
            Rast_get_f_row(fd4, cell4, row);
        if (albedo != NULL)
            Rast_get_f_row(fd5, cell5, row);
        if (latin != NULL)
            Rast_get_f_row(fd6, cell6, row);
        if (longin != NULL)
            Rast_get_f_row(fd7, cell7, row);
        if (coefbh != NULL)
            Rast_get_f_row(fr1, rast1, row);
        if (coefdh != NULL)
            Rast_get_f_row(fr2, rast2, row);
        /*
           if(coeftemp != NULL) 
           {
           G_get_f_raster_row(ft1,rast3,row);
           G_get_f_raster_row(ft2,rast4,row);
           G_get_f_raster_row(ft3,rast5,row);
           G_get_f_raster_row(ft4,rast6,row);


           }
         */
        if (coefwind != NULL) {
            Rast_get_f_row(fw1, rastw1, row);
            Rast_get_f_row(fw2, rastw2, row);
            Rast_get_f_row(fw3, rastw3, row);
            Rast_get_f_row(fw4, rastw4, row);


        }



        row_rev = m - row - 1;
        rowrevoffset = row_rev - offset;

        for (j = 0; j < n; j++) {
            if (!Rast_is_f_null_value(cell1 + j))
                z[rowrevoffset][j] = (float)cell1[j];
            else
                z[rowrevoffset][j] = UNDEFZ;

            if (aspin != NULL) {
                if (!Rast_is_f_null_value(cell2 + j))
                    o[rowrevoffset][j] = (float)cell2[j];
                else
                    o[rowrevoffset][j] = UNDEFZ;
            }
            if (slopein != NULL) {
                if (!Rast_is_f_null_value(cell3 + j))
                    s[rowrevoffset][j] = (float)cell3[j];
                else
                    s[rowrevoffset][j] = UNDEFZ;
            }

            if (linkein != NULL) {
                if (!Rast_is_f_null_value(cell4 + j))
                    li[rowrevoffset][j] = (float)cell4[j];
                else
                    li[rowrevoffset][j] = UNDEFZ;
            }

            if (albedo != NULL) {
                if (!Rast_is_f_null_value(cell5 + j))
                    a[rowrevoffset][j] = (float)cell5[j];
                else
                    a[rowrevoffset][j] = UNDEFZ;
            }

            if (latin != NULL) {
                if (!Rast_is_f_null_value(cell6 + j))
                    la[rowrevoffset][j] = (float)cell6[j];
                else
                    la[rowrevoffset][j] = UNDEFZ;
            }

            if (longin != NULL) {
                if (!Rast_is_f_null_value(cell7 + j))
                    longitArray[rowrevoffset][j] = (float)cell7[j];
                else
                    longitArray[rowrevoffset][j] = UNDEFZ;
            }

            if (coefbh != NULL) {
                if (!Rast_is_f_null_value(rast1 + j))
                    cbhr[rowrevoffset][j] = (float)rast1[j];
                else
                    cbhr[rowrevoffset][j] = UNDEFZ;
            }

            if (coefdh != NULL) {
                if (!Rast_is_f_null_value(rast2 + j))
                    cdhr[rowrevoffset][j] = (float)rast2[j];
                else
                    cdhr[rowrevoffset][j] = UNDEFZ;
            }


            /*
               if(coeftemp != NULL) {

               if(!Rast_is_f_null_value(rast4+j))
               tempCoeff1[rowrevoffset][j] = (float ) rast4[j];
               else 
               tempCoeff1[rowrevoffset][j] = UNDEFZ;
               if(!Rast_is_f_null_value(rast5+j))
               tempCoeff2[rowrevoffset][j] = (float ) rast5[j];
               else 
               tempCoeff2[rowrevoffset][j] = UNDEFZ;
               if(!Rast_is_f_null_value(rast6+j))
               tempCoeff3[rowrevoffset][j] = (float ) rast6[j];
               else 
               tempCoeff3[rowrevoffset][j] = UNDEFZ;
               }
             */
            if (coefwind != NULL) {

                if (!Rast_is_f_null_value(rastw1 + j))
                    windCoeff0[rowrevoffset][j] = (float)rastw1[j];
                else
                    windCoeff2[rowrevoffset][j] = UNDEFZ;
                if (!Rast_is_f_null_value(rastw2 + j))
                    windCoeff1[rowrevoffset][j] = (float)rastw2[j];
                else
                    windCoeff1[rowrevoffset][j] = UNDEFZ;
                if (!Rast_is_f_null_value(rastw3 + j))
                    windCoeff2[rowrevoffset][j] = (float)rastw3[j];
                else
                    windCoeff2[rowrevoffset][j] = UNDEFZ;
                if (!Rast_is_f_null_value(rastw4 + j))
                    windCoeff3[rowrevoffset][j] = (float)rastw4[j];
                else
                    windCoeff3[rowrevoffset][j] = UNDEFZ;
            }


        }
    }

    if (useHorizonData()) {
        for (i = 0; i < arrayNumInt; i++) {
            Rast_close(fd_shad[i]);
            G_free(horizonbuf[i]);
        }
    }
    Rast_close(fd1);
    G_free(cell1);

    if (aspin != NULL) {
        G_free(cell2);
        Rast_close(fd2);
    }
    if (slopein != NULL) {
        G_free(cell3);
        Rast_close(fd3);
    }
    if (linkein != NULL) {
        G_free(cell4);
        Rast_close(fd4);
    }
    if (albedo != NULL) {
        G_free(cell5);
        Rast_close(fd5);
    }
    if (latin != NULL) {
        G_free(cell6);
        Rast_close(fd6);
    }
    if (longin != NULL) {
        G_free(cell7);
        Rast_close(fd7);
    }
    if (coefbh != NULL) {
        G_free(rast1);
        Rast_close(fr1);
    }
    if (coefdh != NULL) {
        G_free(rast2);
        Rast_close(fr2);
    }

    /*
       if(coeftemp != NULL) 
       {
       G_free(rast3);
       Rast_close(ft1);
       G_free(rast4);
       Rast_close(ft2);
       G_free(rast5);
       Rast_close(ft3);
       G_free(rast6);
       Rast_close(ft4);
       }
     */
    if (coefwind != NULL) {
        G_free(rastw1);
        Rast_close(fw1);
        G_free(rastw2);
        Rast_close(fw2);
        G_free(rastw3);
        Rast_close(fw3);
        G_free(rastw4);
        Rast_close(fw4);
    }


/*******transformation of angles from 0 to east counterclock
        to 0 to north clocwise, for ori=0 upslope flowlines
        turn the orientation 2*M_PI ************/

    /* needs to be eliminated */

    /*for (i = 0; i < m; ++i) */
    for (i = 0; i < numRows; i++) {
        for (j = 0; j < n; j++) {
            *zmax = AMAX1(*zmax, z[i][j]);
            if (aspin != NULL) {
                if (o[i][j] != 0.) {
                    if (o[i][j] < 90.)
                        o[i][j] = 90. - o[i][j];
                    else
                        o[i][j] = 450. - o[i][j];
                }
                /*   printf("o,z = %d  %d i,j, %d %d \n", o[i][j],z[i][j],i,j); */

                if ((aspin != NULL) && o[i][j] == UNDEFZ)
                    z[i][j] = UNDEFZ;
                if ((slopein != NULL) && s[i][j] == UNDEFZ)
                    z[i][j] = UNDEFZ;
                if (linkein != NULL && li[i][j] == UNDEFZ)
                    z[i][j] = UNDEFZ;
                if (albedo != NULL && a[i][j] == UNDEFZ)
                    z[i][j] = UNDEFZ;
                if (latin != NULL && la[i][j] == UNDEFZ)
                    z[i][j] = UNDEFZ;
                if (coefbh != NULL && cbhr[i][j] == UNDEFZ)
                    z[i][j] = UNDEFZ;
                if (coefdh != NULL && cdhr[i][j] == UNDEFZ)
                    z[i][j] = UNDEFZ;
            }

        }
    }

    return 1;
}

int OUTGR(void)
{
    FCELL *cell8 = NULL, *cell9 = NULL, *cell10 = NULL, *cell11 =
        NULL, *cell12 = NULL, *cell13 = NULL;
    int fd8 = -1, fd9 = -1, fd10 = -1, fd11 = -1, fd12 = -1, fd13 = -1;

    int i, iarc, j;

    char msg[100];



    if (beam_rad != NULL) {
        cell8 = Rast_allocate_f_buf();
        fd8 = Rast_open_fp_new(beam_rad);
        if (fd8 < 0) {
            G_fatal_error("unable to create raster map %s", beam_rad);
            exit(1);
        }
    }


    if (diff_rad != NULL) {
        cell9 = Rast_allocate_f_buf();
        fd9 = Rast_open_fp_new(diff_rad);
        if (fd9 < 0) {
            G_fatal_error("unable to create raster map %s", diff_rad);
            exit(1);
        }
    }

    if (refl_rad != NULL) {
        cell10 = Rast_allocate_f_buf();
        fd10 = Rast_open_fp_new(refl_rad);
        if (fd10 < 0) {
            G_fatal_error("unable to create raster map %s", refl_rad);
            exit(1);
        }
    }

    if (glob_pow != NULL) {
        cell12 = Rast_allocate_f_buf();
        fd12 = Rast_open_fp_new(glob_pow);
        if (fd12 < 0) {
            G_fatal_error("unable to create raster map %s", glob_pow);
            exit(1);
        }
    }

    if (mod_temp != NULL) {
        cell13 = Rast_allocate_f_buf();
        fd13 = Rast_open_fp_new(mod_temp);
        if (fd13 < 0) {
            G_fatal_error("unable to create raster map %s", mod_temp);
            exit(1);
        }
    }



    if (m != Rast_window_rows()) {
        fprintf(stderr, "OOPS: rows changed from %d to %d\n", m,
                Rast_window_rows());
        exit(1);
    }
    if (n != Rast_window_cols()) {
        fprintf(stderr, "OOPS: cols changed from %d to %d\n", n,
                Rast_window_cols());
        exit(1);
    }

    for (iarc = 0; iarc < m; iarc++) {
        i = m - iarc - 1;

        if (beam_rad != NULL) {
            for (j = 0; j < n; j++) {
                if (beam[i][j] == UNDEFZ)
                    Rast_set_f_null_value(cell8 + j, 1);
                else
                    cell8[j] = (FCELL) beam[i][j];

            }
            Rast_put_f_row(fd8, cell8);
        }

        if (glob_pow != NULL) {
            for (j = 0; j < n; j++) {
                if (globrad[i][j] == UNDEFZ)
                    Rast_set_f_null_value(cell12 + j, 1);
                else
                    cell12[j] = (FCELL) globrad[i][j];

            }
            Rast_put_f_row(fd12, cell12);
        }

        if (mod_temp != NULL) {
            for (j = 0; j < n; j++) {
                if (modtemp_rast[i][j] == UNDEFZ)
                    Rast_set_f_null_value(cell13 + j, 1);
                else
                    cell13[j] = (FCELL) modtemp_rast[i][j];

            }
            Rast_put_f_row(fd13, cell13);
        }



        if (diff_rad != NULL) {
            for (j = 0; j < n; j++) {
                if (diff[i][j] == UNDEFZ)
                    Rast_set_f_null_value(cell9 + j, 1);
                else
                    cell9[j] = (FCELL) diff[i][j];
            }
            Rast_put_f_row(fd9, cell9);
        }

        if (refl_rad != NULL) {
            for (j = 0; j < n; j++) {
                if (refl[i][j] == UNDEFZ)
                    Rast_set_f_null_value(cell10 + j, 1);
                else
                    cell10[j] = (FCELL) refl[i][j];
            }
            Rast_put_f_row(fd10, cell10);
        }

    }

    if (beam_rad != NULL) {
        Rast_close(fd8);
        Rast_write_history(beam_rad, &hist);
    }
    if (diff_rad != NULL) {
        Rast_close(fd9);
        Rast_write_history(diff_rad, &hist);
    }
    if (refl_rad != NULL) {
        Rast_close(fd10);
        Rast_write_history(refl_rad, &hist);
    }
    if (glob_pow != NULL) {
        Rast_close(fd12);
        Rast_write_history(glob_pow, &hist);
    }
    if (mod_temp != NULL) {
        Rast_close(fd13);
        Rast_write_history(mod_temp, &hist);
    }

    return 1;
}

/*  min(), max() are unused
   int min(arg1, arg2)
   int arg1;
   int arg2;
   {
   int res;
   if (arg1 <= arg2) {
   res = arg1;
   }
   else {
   res = arg2;
   }
   return res;
   }

   int max(arg1, arg2)
   int arg1;
   int arg2;
   {
   int res;
   if (arg1 >= arg2) {
   res = arg1;
   }
   else {
   res = arg2;
   }
   return res;
   }
 */




#define C1 1.15
#define C2 -0.32
#define P_STC 94.804
#define V_STC 34.6
#define I_STC 2.74
#define ALPHA_VM 0.0033
#define BETA_VM -0.159
#define T_STC 25.

double modelConstants[7];

int initEfficiencyCoeffs(char *filename, int useWind)
{

    int i;

    FILE *fp;


    modelConstants[0] = 94.804;
    modelConstants[1] = 3.151;
    modelConstants[2] = -0.8768;
    modelConstants[3] = -0.32148;
    modelConstants[4] = 0.003795;
    modelConstants[5] = -0.001056;
    modelConstants[6] = -0.0005247;

    if (!filename)
        return 0;

    fp = fopen(filename, "r");

    if (!fp) {
        printf("Could not open coefficients file %s. Aborting.\n", filename);
        exit(1);
    }
    for (i = 0; i < 8; i++) {
        if (fscanf(fp, "%lf", modelConstants + i) != 1) {
            printf
                ("Could not read coefficient from coefficients file. Aborting.\n");
            exit(1);
        }
    }
    if (useWind)
        if (fscanf(fp, "%lf", modelConstants + 8) != 1) {
            printf
                ("Could not read coefficient from coefficients file. Aborting.\n");
            exit(1);
        }


    fclose(fp);

    return 0;

}

/*  Version with Faiman model for module temperature, not used at the moment.
   double efficiency(double irr, double *temp, double wind)
   {
   double pm;
   double relirr, lnrelirr;
   double tprime;

   relirr = 0.001*irr;

   lnrelirr = log(relirr);

   if(useTemperature()&&useWind())
   *temp+=irr/(modelConstants[7]+modelConstants[8]*wind);
   else
   *temp += modelConstants[7]*irr;

   tprime=*temp-T_STC;

   pm=modelConstants[0]+lnrelirr*(modelConstants[1]+lnrelirr*modelConstants[2]) + tprime*(modelConstants[3]+lnrelirr*(modelConstants[4]+lnrelirr*modelConstants[5])+modelConstants[6]*tprime);

   return pm/modelConstants[0]; 


   }
 */


/* Simple linear model for module temperature */
double efficiency(double irr, double temp)
{
    double pm;

    double relirr, lnrelirr;

    double tprime;

    relirr = 0.001 * irr;

    if (relirr <= 0.)
        return 0.;

    lnrelirr = log(relirr);

    temp = irr * modelConstants[7] + temp;

    tprime = temp - T_STC;

    pm = modelConstants[0] + lnrelirr * (modelConstants[1] +
                                         lnrelirr * modelConstants[2]) +
        tprime * (modelConstants[3] +
                  lnrelirr * (modelConstants[4] +
                              lnrelirr * modelConstants[5]) +
                  modelConstants[6] * tprime);

    return pm / modelConstants[0];
}




void joules2(double *totpower, double *modtemp,
             struct SunGeometryConstDay *sunGeom,
             struct SunGeometryVarDay *sunVarGeom,
             struct SunGeometryVarSlope *sunSlopeGeom,
             struct SolarRadVar *sunRadVar, struct GridGeometry *gridGeom,
             float *temperatureData, double *wcoeffs,
             unsigned char *horizonpointer, double latitude, double longitude,
             BeamRadFunc bRadFunc, DiffRadFunc dRadFunc)
{

    double s0, dfr, dfr_rad;

    double ra, dra;

    double effic = 1.;

    int ss = 1;

    double firstTime, presTime;

    double firstAngle, lastAngle;

    double bh;

    double rr;

    double totRad = 0.;

    double presTemperature, presWindSpeed = 0.;

    double beam_irr, diff_irr, refl_irr;

    double beam_irr_real, diff_irr_real, refl_irr_real;

    double totrad, totrad_real;

    int srStepNo;



    struct SolarRadVar sunRadVar_cs;

    sunRadVar_cs = *sunRadVar;

    if (highIrr) {
        sunRadVar_cs.cbh = 1.;
        sunRadVar_cs.cdh = 1.;
    }

    beam_e = 0.;
    diff_e = 0.;
    refl_e = 0.;
    *totpower = 0.;
    insol_t = 0.;


    com_par(sunGeom, sunVarGeom, gridGeom, latitude, longitude);

    if (tt != NULL) {           /*irradiance */

        s0 = lumcline2(sunGeom, sunVarGeom, sunSlopeGeom, gridGeom,
                       horizonpointer);

        if (sunVarGeom->solarAltitude > 0.) {
            if ((!sunVarGeom->isShadow) && (s0 > 0.)) {
                ra = bRadFunc(s0, &bh, sunVarGeom, sunSlopeGeom, sunRadVar);    /* beam radiation */
                beam_e += ra;
            }
            else {
                beam_e = 0.;
                bh = 0.;
            }

            if ((diff_rad != NULL) || (glob_pow != NULL)) {
                dra = dRadFunc(s0, bh, &rr, sunVarGeom, sunSlopeGeom, sunRadVar);       /* diffuse rad. */
                diff_e += dra;
            }
            if ((refl_rad != NULL) || (glob_pow != NULL)) {
                if ((diff_rad == NULL) && (glob_pow == NULL))
                    dRadFunc(s0, bh, &rr, sunVarGeom, sunSlopeGeom,
                             sunRadVar);
                refl_e += rr;   /* reflected rad. */
            }
            totRad = beam_e + diff_e + refl_e;


            effic = efficiency(totRad, *temperatureData);

            *totpower = effic * totRad;
        }                       /* solarAltitude */
    }
    else {
        /* all-day radiation */


        srStepNo = (int)(sunGeom->sunrise_time / step);

        if ((sunGeom->sunrise_time - srStepNo * step) > 0.5 * step) {
            firstTime = (srStepNo + 1.5) * step;
        }
        else {
            firstTime = (srStepNo + 0.5) * step;
        }

        presTime = firstTime;

        firstAngle = (firstTime - 12) * HOURANGLE;
        lastAngle = (sunGeom->sunset_time - 12) * HOURANGLE;




        dfr_rad = step * HOURANGLE;

        sunGeom->timeAngle = firstAngle;



        dfr = step;

        while (ss == 1) {



            beam_irr = diff_irr = refl_irr = 0.;
            beam_irr_real = diff_irr_real = refl_irr_real = 0.;
            if (useTemperature())
                presTemperature =
                    temperatureInterpolate(temperatureData, presTime,
                                           longitude);
            if (useWind())
                presWindSpeed = temperature(wcoeffs, presTime);


            com_par(sunGeom, sunVarGeom, gridGeom, latitude, longitude);
            s0 = lumcline2(sunGeom, sunVarGeom, sunSlopeGeom, gridGeom,
                           horizonpointer);


            if (sunVarGeom->solarAltitude > 0.) {

                if ((!sunVarGeom->isShadow) && (s0 > 0.)) {
                    insol_t += dfr;
                    ra = bRadFunc(s0, &bh, sunVarGeom, sunSlopeGeom,
                                  &sunRadVar_cs);
                    beam_irr = ra;
                    if ((diff_rad != NULL) || (glob_pow != NULL)) {
                        dra =
                            dRadFunc(s0, bh, &rr, sunVarGeom, sunSlopeGeom,
                                     &sunRadVar_cs);
                        diff_irr = dra;
                    }
                    if ((refl_rad != NULL) || (glob_pow != NULL)) {
                        refl_irr = rr;
                    }
                    ra *= sunRadVar->cbh / sunRadVar_cs.cbh;
                    bh *= sunRadVar->cbh / sunRadVar_cs.cbh;
                    beam_irr_real = ra;
                    beam_e += dfr * ra;
                    ra = 0.;
                }
                else {
                    bh = 0.;
                    if ((diff_rad != NULL) || (glob_pow != NULL)) {
                        dra =
                            dRadFunc(s0, bh, &rr, sunVarGeom, sunSlopeGeom,
                                     &sunRadVar_cs);
                        diff_irr = dra;
                    }
                    if ((refl_rad != NULL) || (glob_pow != NULL)) {
                        refl_irr = rr;
                    }
                }

                if ((diff_rad != NULL) || (glob_pow != NULL)) {
                    dra =
                        dRadFunc(s0, bh, &rr, sunVarGeom, sunSlopeGeom,
                                 sunRadVar);
                    diff_e += dfr * dra;
                    diff_irr_real = dra;
                    dra = 0.;
                }
                if ((refl_rad != NULL) || (glob_pow != NULL)) {
                    dRadFunc(s0, bh, &rr, sunVarGeom, sunSlopeGeom,
                             sunRadVar);
                    refl_irr_real = rr;
                    refl_e += dfr * rr;
                    rr = 0.;
                }
            }                   /* illuminated */


            totrad = beam_irr + diff_irr + refl_irr;
            totrad_real = beam_irr_real + diff_irr_real + refl_irr_real;

            if (useTemperature())
                effic = efficiency(totrad, presTemperature);

            *totpower += effic * totrad_real * dfr;


            sunGeom->timeAngle = sunGeom->timeAngle + dfr_rad;


            if (sunGeom->timeAngle > lastAngle) {
                ss = 0;         /* we've got the sunset */
            }
            presTime += step;
        }                       /* end of while */
    }                           /* all-day radiation */

}

/*////////////////////////////////////////////////////////////////////// */



void where_is_point(double *length, struct SunGeometryVarDay *sunVarGeom,
                    struct GridGeometry *gridGeom)
{
    double sx, sy;

    double dx, dy;

    /*              double adx, ady; */
    int i, j;

    sx = gridGeom->xx0 * invstepx + offsetx;    /* offset 0.5 cell size to get the right cell i, j */
    sy = gridGeom->yy0 * invstepy + offsety;

    i = (int)sx;
    j = (int)sy;

    /*      if (i < n-1  && j < m-1) {    to include last row/col */
    if (i <= n - 1 && j <= m - 1) {

        dx = (double)i *gridGeom->stepx;

        dy = (double)j *gridGeom->stepy;

        *length = DISTANCE1(gridGeom->xg0, dx, gridGeom->yg0, dy);      /* dist from orig. grid point to the current grid point */

        sunVarGeom->zp = z[j][i];

        /*
           cube(j, i);
         */
    }
}





/*////////////////////////////////////////////////////////////////////// */

void calculate(int angleloss, double singleSlope, double singleAspect,
               struct SolarRadVar globalRadValues,
               struct GridGeometry gridGeom)
{
    int i, j, l;

    /*                      double energy; */
    double latid_l, cos_u, cos_v, sin_u, sin_v;

    double sin_phi_l, tan_lam_l;

    double zmax;

    int someRadiation;

    int numRows;

    int arrayOffset;

    double latitude, longitude;

    double q1;

    double locTimeOffset;

    double modtemperature;

    double dayRad;

    double wcoeffs[4];

    double totpower;


    struct SunGeometryConstDay sunGeom;

    struct SunGeometryVarDay sunVarGeom;

    struct SunGeometryVarSlope sunSlopeGeom;

    struct SolarRadVar sunRadVar;

    BeamRadFunc bRadFunc;

    DiffRadFunc dRadFunc;



    if (angleloss) {
        bRadFunc = brad_angle_loss;
        dRadFunc = drad_angle_loss;
    }
    else {
        bRadFunc = brad;
        dRadFunc = drad;
    }

    sunSlopeGeom.slope = singleSlope;
    sunSlopeGeom.aspect = singleAspect;
    sunRadVar.alb = globalRadValues.alb;

    sunGeom.sindecl = sin(declination);
    sunGeom.cosdecl = cos(declination);


    someRadiation = (beam_rad != NULL) || (diff_rad != NULL) ||
        (refl_rad != NULL) || (glob_pow != NULL);


    fprintf(stderr, "\n\n");


    if (beam_rad != NULL) {
        beam = (float **)malloc(sizeof(float *) * (m));
        for (l = 0; l < m; l++) {
            beam[l] = (float *)malloc(sizeof(float) * (n));
        }

        for (j = 0; j < m; j++) {
            for (i = 0; i < n; i++)
                beam[j][i] = UNDEFZ;
        }
    }


    if (diff_rad != NULL) {
        diff = (float **)malloc(sizeof(float *) * (m));
        for (l = 0; l < m; l++) {
            diff[l] = (float *)malloc(sizeof(float) * (n));
        }

        for (j = 0; j < m; j++) {
            for (i = 0; i < n; i++)
                diff[j][i] = UNDEFZ;
        }
    }

    if (refl_rad != NULL) {
        refl = (float **)malloc(sizeof(float *) * (m));
        for (l = 0; l < m; l++) {
            refl[l] = (float *)malloc(sizeof(float) * (n));
        }

        for (j = 0; j < m; j++) {
            for (i = 0; i < n; i++)
                refl[j][i] = UNDEFZ;
        }
    }

    if (glob_pow != NULL) {
        globrad = (float **)malloc(sizeof(float *) * (m));
        for (l = 0; l < m; l++) {
            globrad[l] = (float *)malloc(sizeof(float) * (n));
        }

        for (j = 0; j < m; j++) {
            for (i = 0; i < n; i++)
                globrad[j][i] = UNDEFZ;
        }
    }

    if (mod_temp != NULL) {
        modtemp_rast = (float **)malloc(sizeof(float *) * (m));
        for (l = 0; l < m; l++) {
            modtemp_rast[l] = (float *)malloc(sizeof(float) * (n));
        }

        for (j = 0; j < m; j++) {
            for (i = 0; i < n; i++)
                modtemp_rast[j][i] = UNDEFZ;
        }
    }


    sunRadVar.G_norm_extra = com_sol_const(day);

    numRows = m / numPartitions;


    if (useCivilTime()) {
        /* We need to calculate the deviation of the local solar time from the 
           "local clock time". */
        dayRad = 2. * M_PI * day / 365.25;
        locTimeOffset =
            0.128 * sin(dayRad - 0.04887) + 0.165 * sin(2 * dayRad + 0.34383);

        /* Time offset due to timezone as input by user */

        locTimeOffset += civilTime;
        setTimeOffset(locTimeOffset);
    }
    else {
        setTimeOffset(0.);

    }


    for (j = 0; j < m; j++) {
        G_percent(j, m - 1, 2);

        if (j % (numRows) == 0) {
            INPUT_part(j, &zmax);
            arrayOffset = 0;
            shadowoffset = 0;

        }

        sunVarGeom.zmax = zmax;


        for (i = 0; i < n; i++) {




            gridGeom.xg0 = gridGeom.xx0 = (double)i *gridGeom.stepx;

            gridGeom.yg0 = gridGeom.yy0 = (double)j *gridGeom.stepy;


            gridGeom.xp = xmin + gridGeom.xx0;
            gridGeom.yp = ymin + gridGeom.yy0;

            func = NULL;
            sunVarGeom.z_orig = sunVarGeom.zp = z[arrayOffset][i];


            if (sunVarGeom.z_orig != UNDEFZ) {
                if (aspin != NULL) {
                    if (o[arrayOffset][i] != 0.)
                        sunSlopeGeom.aspect = o[arrayOffset][i] * deg2rad;
                    else
                        sunSlopeGeom.aspect = UNDEF;
                }
                if (slopein != NULL) {
                    sunSlopeGeom.slope = s[arrayOffset][i] * deg2rad;
                }
                if (linkein != NULL) {
                    sunRadVar.linke = li[arrayOffset][i];
                    li_max = AMAX1(li_max, sunRadVar.linke);
                    li_min = AMIN1(li_min, sunRadVar.linke);
                }
                if (albedo != NULL) {
                    sunRadVar.alb = a[arrayOffset][i];
                    al_max = AMAX1(al_max, sunRadVar.alb);
                    al_min = AMIN1(al_min, sunRadVar.alb);
                }
                if (latin != NULL) {
                    latitude = la[arrayOffset][i];
                    la_max = AMAX1(la_max, latitude);
                    la_min = AMIN1(la_min, latitude);
                    latitude *= deg2rad;
                }
                if ((G_projection() != PROJECTION_LL)) {

                    longitude = gridGeom.xp;
                    latitude = gridGeom.yp;

                    if (pj_do_proj(&longitude, &latitude, &iproj, &oproj) < 0) {
                        G_fatal_error("Error in pj_do_proj");
                    }

                    la_max = AMAX1(la_max, latitude);
                    la_min = AMIN1(la_min, latitude);
                }
                else {          /* ll projection */
                    longitude = gridGeom.xp;
                    latitude = gridGeom.yp;
                    la_max = AMAX1(la_max, latitude);
                    la_min = AMIN1(la_min, latitude);
                }
                if (useCivilTime()) {
                    longitTime = -longitude / 15.;
                }
                latitude *= deg2rad;
                longitude *= deg2rad;

                if (coefbh != NULL) {
                    sunRadVar.cbh = cbhr[arrayOffset][i];
                }
                if (coefdh != NULL) {
                    sunRadVar.cdh = cdhr[arrayOffset][i];
                }
                cos_u = cos(M_PI / 2 - sunSlopeGeom.slope);
                sin_u = sin(M_PI / 2 - sunSlopeGeom.slope);
                cos_v = cos(M_PI / 2 + sunSlopeGeom.aspect);
                sin_v = sin(M_PI / 2 + sunSlopeGeom.aspect);

                if (tt != NULL)
                    sunGeom.timeAngle = tim;

                gridGeom.sinlat = sin(-latitude);
                gridGeom.coslat = cos(-latitude);

                sin_phi_l =
                    -gridGeom.coslat * cos_u * sin_v +
                    gridGeom.sinlat * sin_u;
                latid_l = asin(sin_phi_l);

                q1 = gridGeom.sinlat * cos_u * sin_v +
                    gridGeom.coslat * sin_u;
                tan_lam_l = -cos_u * cos_v / q1;
                sunSlopeGeom.longit_l = atan(tan_lam_l);
                sunSlopeGeom.lum_C31_l = cos(latid_l) * sunGeom.cosdecl;
                sunSlopeGeom.lum_C33_l = sin_phi_l * sunGeom.sindecl;


                if (someRadiation) {
                    com_par_const(longitTime, &sunGeom, &gridGeom);
                    sr_min = AMIN1(sr_min, sunGeom.sunrise_time);
                    sr_max = AMAX1(sr_max, sunGeom.sunrise_time);
                    ss_min = AMIN1(ss_min, sunGeom.sunset_time);
                    ss_max = AMAX1(ss_max, sunGeom.sunset_time);

                    joules2(&totpower, &modtemperature,
                            &sunGeom, &sunVarGeom, &sunSlopeGeom,
                            &sunRadVar, &gridGeom,
                            tempdata + 8 * (arrayOffset * n + i), wcoeffs,
                            horizonarray + shadowoffset,
                            latitude, longitude, bRadFunc, dRadFunc);
                    if (beam_rad != NULL)
                        beam[j][i] = (float)beam_e;
                    /*      printf("\n %f",insol[j][i]); */
                    if (diff_rad != NULL)
                        diff[j][i] = (float)diff_e;
                    if (refl_rad != NULL)
                        refl[j][i] = (float)refl_e;
                    if (glob_pow != NULL)
                        globrad[j][i] = (float)(totpower);
                    if (mod_temp != NULL)
                        modtemp_rast[j][i] = (float)(modtemperature);
                }

            }                   /* undefs */
            shadowoffset += arrayNumInt;
        }
        arrayOffset++;

    }
    fprintf(stderr, "\n");

    /* re-use &hist, but try all to initiate it for any case */
    /*   note this will result in incorrect map titles       */
    if (beam_rad != NULL) {
        Rast_write_history(beam_rad, &hist);
    }
    else if (diff_rad != NULL) {
        Rast_write_history(diff_rad, &hist);
    }
    else if (refl_rad != NULL) {
        Rast_write_history(refl_rad, &hist);
    }
    else if (glob_pow != NULL) {
        Rast_write_history(glob_pow, &hist);
    }
    else
        G_fatal_error
            ("Failed to init map history: no output maps requested!");


    /* don't call G_write_history() until after Rast_close() or it just gets overwritten */
}


double com_declin(int no_of_day)
{
    double d1, decl;

    d1 = pi2 * no_of_day / 365.25;
    decl = -asin(0.3978 * sin(d1 - 1.4 + 0.0355 * sin(d1 - 0.0489)));

    return (decl);
}
