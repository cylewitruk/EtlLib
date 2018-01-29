namespace EtlLib.Data
{
    public class NodeOutputWithFilePath : INodeOutput<NodeOutputWithFilePath>, IHasFilePath
    {
        public string FilePath { get; private set; }
        public bool IsFrozen { get; private set; }

        public NodeOutputWithFilePath() { }

        public NodeOutputWithFilePath(string filePath)
        {
            FilePath = filePath;
        }

        public virtual void Reset()
        {
            FilePath = null;
        }
        
        public void Freeze() => IsFrozen = true;

        public virtual void CopyTo(NodeOutputWithFilePath obj)
        {
            obj.FilePath = FilePath;
        }

    }
}