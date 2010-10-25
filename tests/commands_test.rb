require 'commands'
require 'test/unit'

module Commands

  class MockExecutor
    def exec(cmd)
    end
  end

  class MockEMRClient
    attr_accessor :state

    def initialize(config)
      @config = config
      @state = "RUNNING"
    end

    def self.new_aws_query(config)
      return MockEMRClient.new(config)
    end

    def DescribeJobFlows(args)
      return {
        "JobFlows" =>
        [
         {
           "LogUri" => "s3n://testing/", 
           "Name" => "Development Job Flow  (requires manual termination)", 
           "BootstrapActions" =>[], 
           "ExecutionStatusDetail" => {
             "EndDateTime" => 1286584312.0, 
             "CreationDateTime" => 1286584224.0, 
             "LastStateChangeReason" => "Terminated by user request", 
             "State" => @state, 
             "StartDateTime" => nil, 
             "ReadyDateTime" => nil
           }, 
           "Steps" =>[], 
           "JobFlowId" => "j-2HWO50OUKNMHG", 
           "Instances" => {
             "Ec2KeyName" => "richcole-test", 
             "InstanceCount" =>5, 
             "NormalizedInstanceHours" => nil, 
             "Placement" => {"AvailabilityZone" => "us-east-1d"}, 
             "KeepJobFlowAliveWhenNoSteps" => true, 
             "SlaveInstanceType" => "m2.xlarge", 
             "MasterInstanceType" => "m2.xlarge", 
             "MasterPublicDnsName" => nil, 
             "MasterInstanceId" => nil, 
             "InstanceGroups" => [{
               "SpotPrice" => nil,
               "EndDateTime" => nil,
               "Name" => "Task Instance Group",
               "InstanceRole" => "TASK",
               "CreationDateTime" => 1286862675.0,
               "LaunchGroup" => nil,
               "LastStateChangeReason" => "",
               "InstanceGroupId" => "ig-D2NC23WFSOOU",
               "State" => "RUNNING",
               "Market" => "ON_DEMAND",
               "InstanceType" => "c1.medium",
               "StartDateTime" => 1286862907.0,
               "InstanceRunningCount" => 2,
               "ReadyDateTime" => 1286862907.0,
               "InstanceRequestCount" => 2
               },
               {
               "SpotPrice" => nil,
               "EndDateTime" => nil,
               "Name" => "Master Instance Group",
               "InstanceRole" => "MASTER",
               "CreationDateTime" => 1286862675.0,
               "LaunchGroup" => nil,
               "LastStateChangeReason" => "",
               "InstanceGroupId" => "ig-1BFN7TCX7YE5Y",
               "State" => "RUNNING",
               "Market" => "ON_DEMAND",
               "InstanceType" => "m1.small",
               "StartDateTime" => 1286862866.0,
               "InstanceRunningCount" => 1,
               "ReadyDateTime" => 1286862906.0,
               "InstanceRequestCount" => 1
               },
               {
               "SpotPrice" => nil,
               "EndDateTime" => nil,
               "Name" => "Core Instance Group",
               "InstanceRole" => "CORE",
               "CreationDateTime" => 1286862675.0,
               "LaunchGroup" => nil,
               "LastStateChangeReason" => "Expanding cluster",
               "InstanceGroupId" => "ig-2EUIGTIPDLTXW",
               "State" => "RESIZING",
               "Market" => "ON_DEMAND",
               "InstanceType" => "m1.large",
               "StartDateTime" => 1286862907.0,
               "InstanceRunningCount" => 1,
               "ReadyDateTime" => 1286862907.0,
               "InstanceRequestCount" => 3
               }]
           },
           "HadoopVersion" => "0.20"
         } 
        ]
      }
    end

    def RunJobFlow(opts)
      return { "JobFlowId" => "j-ABABABABA" }
    end

    def AddJobFlowSteps(opts)
      return nil
    end

    def TerminateJobFlows(opts)
      return nil
    end

    def ModifyInstanceGroups(opts)
      return nil
    end

    def AddInstanceGroups(opts)
      return nil
    end

  end

  class MockLogger
    def puts(msg)
    end

    def trace(msg)
    end
  end

  class CommandTest < Test::Unit::TestCase

    def setup
      @client_class = MockEMRClient #FIXME: make this return always the same object
      @logger       = MockLogger.new
      @executor     = MockExecutor.new
    end

    def create_and_execute_commands(args)
      return ::Commands.create_and_execute_commands(args.split(/\s+/), @client_class, @logger, @executor, false)
    end

    def test_modify_instance_group_command
      args = "-c tests/credentials.json --modify-instance-group core --instance-count 10 --jobflow j-ABABABA"
      @commands = create_and_execute_commands(args)
      assert_equal(1, @commands.size)
      c = @commands.last
      assert(c.is_a? ModifyInstanceGroupCommand)
      assert_equal(10, c.instance_count)
      assert_not_nil(c.instance_group_id)
      assert_equal(nil, c.instance_type)
      assert_equal("CORE", c.instance_role)
    end

    def test_one
      args = "-c tests/credentials.json --create --alive --num-instances 10 " +
        "--slave-instance-type m1.small --master-instance-type m1.large"
      @commands = create_and_execute_commands(args)
    end

    def test_two
      args = "-c tests/credentials.json --create --alive --num-instances 10 " +
        "--slave-instance-type m1.small --master-instance-type m1.large " + 
        "--instance-group TASK --instance-type m1.small --instance-count 10 " + 
        "--bootstrap-action s3://elasticmapreduce/scripts/configure-hadoop " + 
        "--arg s3://mybucket/config/custom-site-config.xml "
      @commands = create_and_execute_commands(args)
      assert_equal(1, @commands.commands.size)
    end

    def test_three
      args = "-c tests/credentials.json --create --alive --num-instances 10 " + 
        "--slave-instance-type m1.small --master-instance-type m1.large " +
        "--instance-group TASK --instance-type m1.small --instance-count 10 " + 
        "--bootstrap-action s3://elasticmapreduce/scripts/configure-hadoop " + 
        "--arg s3://mybucket/config/custom-site-config.xml " +
        "--pig-script s3://elasticmapreduce/samples/sample.pig " + 
        "--pig-interactive"
      @commands = create_and_execute_commands(args)
      assert_equal(1, @commands.commands.size)
      cmd1 = @commands.commands.first
      assert_equal(2, cmd1.step_commands.size)
      assert_equal(PigInteractiveCommand, cmd1.step_commands[0].class)
    end

    def test_four
      args = "-a ACCESS_ID -p SECRET_KEY --create --alive " + 
        "--hive-script s3://maps.google.com --enable-debugging " + 
        "--log-uri s3://somewhere.com/logs/"
      @commands = create_and_execute_commands(args)
      assert_equal(1, @commands.commands.size)
      assert_equal(3, @commands.commands[0].step_commands.size)
      steps = @commands.commands[0].step_commands
      assert_equal(EnableDebuggingCommand, steps[0].class) 
      assert_equal(HiveInteractiveCommand, steps[1].class) 
      assert_equal(HiveScriptCommand, steps[2].class) 
    end

    def test_five
      args = "-a ACCESS_ID -p SECRET_KEY -j j-ABABABAABA --hive-script " + 
        "s3://maps.google.com --enable-debugging --log-uri s3://somewhere.com/logs/"
      @commands = create_and_execute_commands(args)
      assert_equal(1, @commands.commands.size)
      assert_equal(3, @commands.commands[0].step_commands.size)
      steps = @commands.commands[0].step_commands
      assert_equal(EnableDebuggingCommand, steps[0].class) 
      assert_equal(HiveInteractiveCommand, steps[1].class) 
      assert_equal(HiveScriptCommand, steps[2].class) 
    end

    def test_six
      args = "-a ACCESS_ID -p SECRET_KEY --list --active"
      @commands = create_and_execute_commands(args)
    end

    def test_seven
      args = "-a ACCESS_ID -p SECRET_KEY --list --active --terminate"
      @commands = create_and_execute_commands(args)
    end

    def test_eight
      args = "-a ACCESS_ID -p SECRET_KEY --terminate -j j-ABABABABA"
      @commands = create_and_execute_commands(args)
    end

    def test_create_one
      args = "-a ACCESS_ID -p SECRET_KEY --create --alive --name TestFlow"
      @commands = create_and_execute_commands(args)
    end

    def test_ssh_no_jobflow
      args = "-a ACCESS_ID -p SECRET_KEY --ssh"
      assert_raise RuntimeError do
        @commands = create_and_execute_commands(args)
      end
    end

    def test_ssh_too_many_jobflows
      args = "-a ACCESS_ID -p SECRET_KEY -j j-ABABABA j-ABABABA --ssh"
      assert_raise RuntimeError do
        @commands = create_and_execute_commands(args)
      end
    end

    def test_ssh
      args = "-a ACCESS_ID -p SECRET_KEY --key-pair-file test.pem -j j-ABABABA --ssh"
      @commands = create_and_execute_commands(args)
    end

    def test_unarrest
      args = "-a ACCESS_ID -p SECRET_KEY --unarrest-instance-group core -j j-ABABABA"
      @commands = create_and_execute_commands(args)
    end

    def test_late_name
      args = "--create --alive --enable-debugging --hive-interactive --name MyHiveJobFlow"
      @commands = create_and_execute_commands(args)
      assert_equal(1, @commands.commands.size)
      assert_equal("MyHiveJobFlow", @commands.commands.first.jobflow_name)
    end

    def test_ic_it
      args = "--create --alive --enable-debugging --hive-interactive --instance-count 5 --instance-type m1.small --name MyHiveJobFlow"
      @commands = create_and_execute_commands(args)
      assert_equal(1, @commands.commands.size)
      cc = @commands.commands.first
      assert_equal("MyHiveJobFlow", cc.jobflow_name)
      assert_equal(5, cc.instance_count)
      assert_equal("m1.small", cc.instance_type)
    end

    def test_command_option_mismatch
      args = "-c tests/credentials.json --instance-group core --instance-count 10"
      assert_raise RuntimeError do
        @commands = create_and_execute_commands(args)
      end
    end

  end
end
