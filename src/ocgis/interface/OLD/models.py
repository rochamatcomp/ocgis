from element import VariableElement, AttributeElement, ElementNotFound
import netCDF4 as nc
import numpy as np
import datetime
from warnings import warn


class RowBounds(VariableElement):
    _names = ['bounds_latitude',
              'bnds_latitude',
              'latitude_bounds',
              'lat_bnds']
    _ocg_name = 'latitude_bounds'
    _iname = 's_row_bounds'
    
    
class ColumnBounds(VariableElement):
    _names = ['bounds_longitude',
              'bnds_longitude',
              'longitude_bounds',
              'lon_bnds']
    _ocg_name = 'longitude_bounds'
    _iname = 's_column_bounds'
    
    
class Row(VariableElement):
    _names = ['latitude','lat']
    _ocg_name = 'latitude'
    _iname = 's_row'
    
    
class Column(VariableElement):
    _names = ['longitude','lon']
    _ocg_name = 'longitude'
    _iname = 's_column'


class Calendar(AttributeElement):
    _names = ['calendar','time_Convention']
    _ocg_name = 'calendar'
    _default = 'proleptic_gregorian'
    _iname = 't_calendar'
    
    def __init__(self,*args,**kwds):
        self._mode = 'local'
        super(Calendar,self).__init__(*args,**kwds)
    
    def _get_name_(self,dataset):
        try:
            ret = super(Calendar,self)._get_name_(dataset)
        except ElementNotFound:
            self._mode = 'global'
            ret = super(Calendar,self)._get_name_(dataset)
        return(ret)
    
    def _possible_(self,dataset):
        if self._mode == 'local':
            ret = dataset.variables[self._parent.name].ncattrs()
        else:
            ret = dataset.ncattrs()
        return(ret)
    
    def _get_value_(self,dataset):
        if self._mode == 'local':
            try:
                ret = getattr(dataset.variables[self._parent.name],self.name)
            except AttributeError:
                ret = self.name
        else:
            ret = getattr(dataset,self.name)
        return(ret)
    
    
class TimeUnits(AttributeElement):
    _names = ['units']
    _ocg_name = 'units'
    _iname = 't_units'


class Time(VariableElement):
    _names = ['time','time_gmo']
    _ocg_name = 'time'
    _AttributeElements = [Calendar,TimeUnits]
    _calendar_map = {'Calandar is no leap':'noleap',
                     'Calandar is actual':'noleap'}
    _iname = 't_variable'
    
    def calculate(self,values):
        ret = nc.date2num(values,self.units.value,calendar=self.calendar.value)
        return(ret)
        
    def _format_(self,timevec):
        time_units = self.units.value
        calendar = self.calendar.value
        try:
            ret = nc.num2date(timevec,units=time_units,calendar=calendar)
        except ValueError as e:
            try:
                new_calendar = self._calendar_map[calendar]
            except KeyError:
                raise(e)
            warn('calendar name "{0}" remapped to "{1}"'.format(calendar,
                                                                new_calendar))
            ret = nc.num2date(timevec,units=time_units,calendar=new_calendar)
        if not isinstance(ret[0],datetime.datetime):
            reformat_timevec = np.empty(ret.shape,dtype=object)
            for ii,t in enumerate(ret):
                reformat_timevec[ii] = datetime.datetime(t.year,t.month,t.day,
                                                         t.hour,t.minute,t.second)
            ret = reformat_timevec
        return(ret)


class Level(VariableElement):
    _names = ['level','lvl','levels','lvls','lev','threshold','plev','plevel','plvl']
    _ocg_name = 'level'
    _iname = 'l_variable'