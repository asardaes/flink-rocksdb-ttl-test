package com.asardaes.flink.dto;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@TypeInfo(StringList.TypeInformationFactory.class)
public class StringList implements Serializable {
    public List<String> values = new ArrayList<>();

    public StringList() {
    }

    public StringList(List<String> values) {
        this.values = values;
    }

    public static class TypeInformationFactory extends TypeInfoFactory<StringList> {
        @Override
        public TypeInformation<StringList> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            Map<String, TypeInformation<?>> fields = new LinkedHashMap<>();
            fields.put("values", Types.LIST(Types.STRING));
            return Types.POJO(StringList.class, fields);
        }
    }
}
