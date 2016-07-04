package teclan.activejdbc.model;

import java.util.Map;

import org.javalite.activejdbc.Model;

import teclan.activejdbc.utils.GsonUtils;
import teclan.activejdbc.utils.Strings;

public abstract class AbstractModel extends Model {

    public String toJson() {
        return GsonUtils.toJson(toMap());
    }

    public String toJson(String root) {
        return GsonUtils.toJson(toMap(), root);
    }

    public String toJson(String... excludedAttributes) {
        return GsonUtils.toJson(toMap(excludedAttributes));
    }

    public String toJson(String root, String... excludedAttributes) {
        return GsonUtils.toJson(toMap(excludedAttributes), root);
    }

    public Map<String, Object> toMap(String... excludedAttributes) {
        Map<String, Object> map = super.toMap();

        for (String attribute : excludedAttributes) {
            map.remove(attribute);
        }

        return map;
    }

    @SuppressWarnings("unchecked")
    public static <T extends AbstractModel> T fromJson(String json, String root,
            Class<T> clazz) {
        Map<String, Object> map = null;
        if (Strings.isEmpty(root)) {
            map = GsonUtils.fromJson(json, Map.class);
        } else {
            map = GsonUtils.fromJson(json, root, Map.class);
        }

        try {
            T instance = clazz.newInstance();
            instance.fromMap(map);

            return instance;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * @author Teclan
     * 
     *         获取通过 @Table 配置的表名
     * 
     * @return 配置表名,并不是activeJdbc自动映射表,虽然这两个值很多时候是一样
     */
    public abstract String getConfigTableName();
}
