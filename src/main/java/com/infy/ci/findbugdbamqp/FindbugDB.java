package com.infy.ci.findbugdbamqp;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;

@Component
public class FindbugDB implements CIData {
	
	@Autowired
	FindBugQueries fb;
	
	public FindbugDB()
	{
		
	}
	
	public FindbugDB(int projectid)
	{
		fb = new FindBugQueries(projectid);
	}
	
	public void setProjectid(int projectid) {

		fb.setProjectid(projectid);
	}
	
	

	@Override
	public String getAggregatedDataForBuild(int buildno) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAggregatedDataForLatestBuild() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAggregatedDataForNightlyBuild(int buildno)
			throws IOException {
		
		List<Map<String, Object>> data;
		ChartData d = new ChartData();
		Gson gson = new Gson();
		List<String> arrayList = new ArrayList<String>();
		arrayList.add("High");
		arrayList.add("Medium");
		
		arrayList.add("Low");
		
		int high = 0;
		int medium = 0;
		int total = 0;
		int low = 0;
		String json;
		
		ArrayList<Integer> singleList = new ArrayList<Integer>();
		
		try {
			data = fb.getfbspecificbldno(buildno);
			if (data.size()!= 0 )
			{
			for (Map<String, Object> data1 : data) {
			    for (Map.Entry<String, Object> entry : data1.entrySet()) {
			       System.out.println(entry.getKey() + ": " + entry.getValue());
			       
			       
			       if (entry.getKey().equals("highwarnings"))
			       {
			    	    high = Integer.parseInt(entry.getValue().toString());
			       }
			       else if (entry.getKey().equals("normalwarnings"))
			       {
			    	    medium = Integer.parseInt(entry.getValue().toString());
			       }
			       
			       else if (entry.getKey().equals("lowwarnings"))
			       {
			    	    low = Integer.parseInt(entry.getValue().toString());
			       }
			       
			    }
			    singleList.add(high);
			    singleList.add(medium);
			    
			    singleList.add(low);
			    
			    Map<String, Object> map = new HashMap<>();
				map.put("Data", singleList);
				
				ArrayList<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
				dataList.add(map);
				
			    d.setCategories(arrayList);
			    d.setData(dataList);
			    
			    json = gson.toJson(d);
			    return json;
			    
			}
			}
			else
			{
				return null;
			}
		} catch (SQLException | ClassNotFoundException e) {
			throw new IOException("Failed to fetch data for unit test",e);
		}
    
		return null;

		
	}

	@Override
	public String getAggregatedDataForLatestNightlyBuild() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAllModulesAggregatedDataForLatestBuild()
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAllModulesAggregatedDataForLatestNightlyBuild()
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAllModuleDataForBuild(int buildno) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getModuleDataForLatestBuild() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getModuleDataForBuild(int buildno) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAllModuleDataForLatestNightlyBuild() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAllModuleDataForNightlyBuild(int buildno)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getModuleDataForLatestNightlyBuild() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getModuleDataForNightlyBuild(int buildno) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAllModuleDataForLatestBuild() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getLatestNightlyaggregate() throws IOException {
		List<Map<String, Object>> data;
		ChartData d = new ChartData();
		Gson gson = new Gson();
		List<String> arrayList = new ArrayList<String>();
		arrayList.add("High");
		arrayList.add("Medium");
		
		arrayList.add("Low");
		
		int high = 0;
		int medium = 0;
		int total = 0;
		int low = 0;
		String json;
		
		ArrayList<Integer> singleList = new ArrayList<Integer>();
		
		try {
			data = fb.getAggregatedFindBugDataForLatestNightlyBuild();
			if (data.size()!= 0 )
			{
			for (Map<String, Object> data1 : data) {
			    for (Map.Entry<String, Object> entry : data1.entrySet()) {
			       System.out.println(entry.getKey() + ": " + entry.getValue());
			       
			       
			       if (entry.getKey().equals("highwarnings"))
			       {
			    	    high = Integer.parseInt(entry.getValue().toString());
			       }
			       else if (entry.getKey().equals("normalwarnings"))
			       {
			    	    medium = Integer.parseInt(entry.getValue().toString());
			       }
			       
			       else if (entry.getKey().equals("lowwarnings"))
			       {
			    	    low = Integer.parseInt(entry.getValue().toString());
			       }
			       
			    }
			    
			    singleList.add(high);
			    singleList.add(medium);
			    
			    singleList.add(low);
			    
			    Map<String, Object> map = new HashMap<>();
				map.put("Data", singleList);
				
				ArrayList<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
				dataList.add(map);
				
			    d.setCategories(arrayList);
			    d.setData(dataList);
			    
			    json = gson.toJson(d);
			    return json;
			    
			}
			}
			else
			{
				return null;
			}
		} catch (SQLException | ClassNotFoundException e) {
			throw new IOException("Failed to fetch data for unit test",e);
		}
    	//return getJSONData(data,true);
		
		// TODO Auto-generated method stub
		return null;

	}

	@Override
	public String getLatestCiModulewise() throws IOException {
		
		List<Map<String, Object>> data;
		try {
			data = fb.getAllModulesFindBugForLatestBuild();
		} catch (SQLException | ClassNotFoundException e) {
			throw new IOException("Failed to fetch data for unit test",e);
		}
    	
    	return getJSONDataForColumnwise(data);
		
	}
	
	private String getJSONDataForColumnwise(List<Map<String, Object>> data)
			throws JsonProcessingException, IOException {
		if(null != data){    		
			Map<String,String> selectDataList = new HashMap<String,String>();
    		selectDataList.put("highwarnings","High");
    		selectDataList.put("normalwarnings","Medium");
    		selectDataList.put("lowwarnings","Low");
    		return FindbugHelper.getInstance().getJSONDataForChartColumnWise(data, "modulename", selectDataList);
    	}else{
    		throw new IOException("Build data for specified build id not found");
    	}
	}


	@Override
	public String getTrendWeekData() throws IOException {
		ChartData d = new ChartData();
		Gson gson = new Gson();
		
		ObjectClass highwarn = new ObjectClass("HighWarnings");
		ObjectClass lowwarn = new ObjectClass("LowWarnings");
		ObjectClass medwarn = new ObjectClass("MediumWarnings");
		ObjectClass totalwarn = new ObjectClass("TotalWarnings");
		
		List<ObjectClass> result = new ArrayList<ObjectClass>();
		String json;
		 
	        List<Integer> arrayList = new ArrayList<Integer>();
	        
	        List<Map<String, Object>> data;
	        Map<String, Object> map1;
	        Map<String, Object> map2;
	        
			try {
				data = fb.getWeekFbAggregateDataNightlyBuild();
			} catch (SQLException | ClassNotFoundException e) {
				throw new IOException("Failed to fetch data for findbug",e);
			}
			
			for (Map<String, Object> data1 : data) {
			    for (Map.Entry<String, Object> entry : data1.entrySet()) {
			       System.out.println(entry.getKey() + ": " + entry.getValue());
			       
			       if (entry.getKey().equals("buildnumber"))
			       {
			    	   arrayList.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       
			       else if (entry.getKey().equals("highwarnings"))
			       {
			    	   highwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       
			       else if (entry.getKey().equals("normalwarnings"))
			       {
			    	   medwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       else if (entry.getKey().equals("lowwarnings"))
			       {
			    	   lowwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       
			       else if (entry.getKey().equals("totalwarnings"))
			       {
			    	   totalwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			
	}
	
	}
			result.add(highwarn);
			result.add(medwarn);
			result.add(lowwarn);
			result.add(totalwarn);
			d.setCategories(arrayList);
			d.setData(result);
			json = gson.toJson(d);
			return json;

		
	}

	

	@Override
	public String getTrendMonthData() throws IOException {

		ChartData d = new ChartData();
		Gson gson = new Gson();
		
		ObjectClass highwarn = new ObjectClass("HighWarnings");
		ObjectClass lowwarn = new ObjectClass("LowWarnings");
		ObjectClass medwarn = new ObjectClass("MediumWarnings");
		ObjectClass totalwarn = new ObjectClass("TotalWarnings");
		
		List<ObjectClass> result = new ArrayList<ObjectClass>();
		String json;
		 
	        List<Integer> arrayList = new ArrayList<Integer>();
	        
	        List<Map<String, Object>> data;
	        Map<String, Object> map1;
	        Map<String, Object> map2;
	        
			try {
				data = fb.getMonthFbAggregateDataNightlyBuild();
			} catch (SQLException | ClassNotFoundException e) {
				throw new IOException("Failed to fetch data for findbug",e);
			}
			
			for (Map<String, Object> data1 : data) {
			    for (Map.Entry<String, Object> entry : data1.entrySet()) {
			       System.out.println(entry.getKey() + ": " + entry.getValue());
			       
			       if (entry.getKey().equals("buildnumber"))
			       {
			    	   arrayList.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       
			       else if (entry.getKey().equals("highwarnings"))
			       {
			    	   highwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       
			       else if (entry.getKey().equals("normalwarnings"))
			       {
			    	   medwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       else if (entry.getKey().equals("lowwarnings"))
			       {
			    	   lowwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       
			       else if (entry.getKey().equals("totalwarnings"))
			       {
			    	   totalwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			
	}
	
	}
			result.add(highwarn);
			result.add(medwarn);
			result.add(lowwarn);
			result.add(totalwarn);
			d.setCategories(arrayList);
			d.setData(result);
			json = gson.toJson(d);
			return json;


	}

	

	@Override
	public String getTrendCustomData(String todate, String fromdate)
			throws IOException {
		// TODO Auto-generated method stub
		
		ChartData d = new ChartData();
		Gson gson = new Gson();
		
		ObjectClass highwarn = new ObjectClass("HighWarnings");
		ObjectClass lowwarn = new ObjectClass("LowWarnings");
		ObjectClass medwarn = new ObjectClass("MediumWarnings");
		ObjectClass totalwarn = new ObjectClass("TotalWarnings");
		
		List<ObjectClass> result = new ArrayList<ObjectClass>();
		String json;
		 
	        List<Integer> arrayList = new ArrayList<Integer>();
	        
	        List<Map<String, Object>> data;
	        Map<String, Object> map1;
	        Map<String, Object> map2;
	        
			try {
				data = fb.getTrendCustomFbData(todate,fromdate);
			} catch (SQLException | ClassNotFoundException e) {
				throw new IOException("Failed to fetch data for findbug",e);
			}
			
			for (Map<String, Object> data1 : data) {
			    for (Map.Entry<String, Object> entry : data1.entrySet()) {
			       System.out.println(entry.getKey() + ": " + entry.getValue());
			       
			       if (entry.getKey().equals("buildnumber"))
			       {
			    	   arrayList.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       
			       else if (entry.getKey().equals("highwarnings"))
			       {
			    	   highwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       
			       else if (entry.getKey().equals("normalwarnings"))
			       {
			    	   medwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       else if (entry.getKey().equals("lowwarnings"))
			       {
			    	   lowwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			       
			       else if (entry.getKey().equals("totalwarnings"))
			       {
			    	   totalwarn.data.add(Integer.parseInt(entry.getValue().toString()));
			       }
			
	}
	
	}
			result.add(highwarn);
			result.add(medwarn);
			result.add(lowwarn);
			result.add(totalwarn);
			d.setCategories(arrayList);
			d.setData(result);
			json = gson.toJson(d);
			return json;


	}


	@Override
	public void setBuildNumber(int buildnumber) {
		// TODO Auto-generated method stub
		
	}



	@Override
	public String getbuildwiseinfo(int projectid, int buildnumber)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public String getProjectNames() throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public String getLatestNightlybuilds() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}	

}
