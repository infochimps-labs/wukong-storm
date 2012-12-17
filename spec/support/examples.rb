Wukong.processor(:simple) do  
  def process(record)
    yield record
  end
end

Wukong.processor(:skipped) do  
  def process(record)
    # skip records
  end
end

Wukong.processor(:multi) do  
  def process(record)
    3.times{ yield record }
  end
end

Wukong.processor(:test_example) do  
  def process(record)
    yield "I raised the #{record['foo']}"
  end
end

Wukong.dataflow(:flow) do
  from_json | test_example
end
