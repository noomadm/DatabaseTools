package th.co.ktb.fraud.monitas.repositories;

import java.util.List;
import java.util.Map;

import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface SourceRepository {

	@Query(nativeQuery = true,
			value = "select column_name,udt_name\n"
					+ "    from INFORMATION_SCHEMA.COLUMNS \n"
					+ "    where table_name = :tablename\n"
					+ "order by ordinal_position")
	public List<Map<String,Object>> getTableDescription(String tablename);
	
}
